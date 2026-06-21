# Rebuild The Kafka Codex ETL From Scratch

This tutorial teaches the codebase by rebuilding it from an empty directory into the current ETL project.

The final project has two independent Python components:

1. A Kafka control-message consumer that reads messages, converts them to a canonical JSON file, and optionally triggers the next component.
2. An Elasticsearch lookup script that reads a converted message, raw JSON object, query JSON object, or direct ID, then searches an Elasticsearch index.

There is also a small Kafka producer for integration testing, environment-specific config files, bootstrap scripts, tests, and a Docker Compose setup for local Kafka and Elasticsearch.

The best way to read this document is to build one stage at a time, run the test or command at the end of that stage, and only then continue.

## 0. Target Project Shape

By the end, the project looks like this:

```text
kafka-codex/
  config/
    local.toml
    dev.toml
    prod.toml
  docs/
    rebuild-from-scratch-tutorial.md
  samples/
    control_message.json
  scripts/
    bootstrap_consumer.sh
    bootstrap_es_lookup.sh
    bootstrap_e2e_docker.sh
  src/
    etl/
      __init__.py
      config.py
      es_lookup.py
      json_io.py
      kafka_consumer.py
      kafka_producer.py
      message.py
  tests/
    test_es_lookup.py
    test_message.py
  docker-compose.yml
  pyproject.toml
  README.md
```

## 1. Create The Empty Project

Start with a new folder:

```bash
mkdir kafka-codex
cd kafka-codex
mkdir -p src/etl tests config samples scripts docs
```

Create a virtual environment:

```bash
python3.11 -m venv .venv
source .venv/bin/activate
```

If Python 3.11 is not installed locally, Python 3.12 can run the code too, but the project target is Python 3.11+.

Create `.gitignore`:

```gitignore
__pycache__/
*.py[cod]
.pytest_cache/
.venv/
data/converted/
```

Line by line:

- `__pycache__/` ignores Python bytecode directories.
- `*.py[cod]` ignores compiled Python files.
- `.pytest_cache/` ignores pytest runtime cache.
- `.venv/` ignores the local virtual environment.
- `data/converted/` ignores generated converted Kafka messages.

Create `pyproject.toml`:

```toml
[project]
name = "kafka-codex-etl"
version = "0.1.0"
description = "Two-component ETL pipeline for Kafka control messages and Elasticsearch lookups."
requires-python = ">=3.11"
dependencies = [
  "elasticsearch>=8.13,<9",
  "kafka-python>=2.0.2,<3",
]

[project.optional-dependencies]
dev = [
  "pytest>=8,<9",
]

[tool.pytest.ini_options]
pythonpath = ["src"]
testpaths = ["tests"]
```

Line by line:

- `[project]` starts the package metadata section.
- `name` is the installable package name.
- `version` gives the package a local version.
- `description` documents the project purpose.
- `requires-python = ">=3.11"` prevents accidental installs on older Python versions.
- `dependencies` lists runtime packages. `elasticsearch` is used by the lookup component. `kafka-python` is used by the producer and consumer.
- `[project.optional-dependencies]` defines extra install groups.
- `dev` adds tools used only while developing.
- `[tool.pytest.ini_options]` configures pytest.
- `pythonpath = ["src"]` lets tests import `etl` without manually setting `PYTHONPATH`.
- `testpaths = ["tests"]` tells pytest where tests live.

Install the project:

```bash
python -m pip install -e ".[dev]"
```

Checkpoint:

```bash
python -m pytest
```

At this exact point pytest may say no tests were collected. That is fine. We have only created the packaging shell.

## 2. Create The Python Package

Create `src/etl/__init__.py`:

```python
"""Kafka and Elasticsearch ETL components."""

__all__ = ["config", "kafka_consumer", "es_lookup"]
```

Line by line:

- The module docstring explains the package purpose.
- `__all__` lists the primary modules. This is not required for imports, but it gives readers a concise map of the package.

Checkpoint:

```bash
python -m compileall src
```

This catches syntax errors before the project has real behavior.

## 3. Add Environment Config Files

The project uses TOML because Python 3.11 includes `tomllib` in the standard library. That gives us structured config without needing PyYAML or another parser.

Create `config/local.toml`:

```toml
[pipeline]
environment = "local"

[kafka]
bootstrap_servers = ["localhost:9092"]
topic = "control-topic"
group_id = "etl-local"
client_id = "etl-control-consumer-local"
auto_offset_reset = "earliest"
enable_auto_commit = true
security_protocol = "PLAINTEXT"

[consumer]
output_dir = "data/converted"
trigger_next = true
next_command = ["python", "-m", "etl.es_lookup"]

[message]
id_attribute = "event_id"

[elasticsearch]
hosts = ["http://localhost:9200"]
index = "source-index"
request_timeout_seconds = 30
verify_certs = false
username = ""
password = ""
api_key = ""
```

Line by line:

- `[pipeline]` stores metadata about the selected environment.
- `environment = "local"` names this config.
- `[kafka]` stores Kafka client settings.
- `bootstrap_servers` points the client to a broker.
- `topic` is the control topic the consumer listens to and the producer publishes to.
- `group_id` is the Kafka consumer group.
- `client_id` identifies this client in Kafka logs.
- `auto_offset_reset = "earliest"` makes local tests consume old messages when no committed offset exists.
- `enable_auto_commit = true` lets the Kafka client commit offsets.
- `security_protocol = "PLAINTEXT"` matches the local Docker broker.
- `[consumer]` stores behavior for the consumer component.
- `output_dir` is where converted JSON files are saved.
- `trigger_next = true` means the consumer should invoke the next script by default.
- `next_command` is the command used to run the Elasticsearch lookup component.
- `[message]` stores message parsing assumptions.
- `id_attribute = "event_id"` says the control message ID field is called `event_id`.
- `[elasticsearch]` stores Elasticsearch client settings.
- `hosts` points to the ES node.
- `index` is the index searched by the lookup script.
- `request_timeout_seconds` protects live calls from hanging forever.
- `verify_certs = false` is convenient for local HTTP.
- `username`, `password`, and `api_key` are empty for local unauthenticated ES.

Create `config/dev.toml` and `config/prod.toml` by copying `local.toml` and changing host names, index names, security protocol, TLS, and auth.

For a real environment, these are the most important fields:

```toml
[kafka]
bootstrap_servers = ["your-kafka-host:9092"]
topic = "your-control-topic"
group_id = "your-consumer-group"
security_protocol = "PLAINTEXT"

[message]
id_attribute = "event_id"

[elasticsearch]
hosts = ["http://your-es-host:9200"]
index = "your-index-name"
verify_certs = true
username = ""
password = ""
api_key = ""
```

## 4. Build The Config Loader

Create `src/etl/config.py`:

```python
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import tomllib
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_DIR = PROJECT_ROOT / "config"


@dataclass(frozen=True)
class AppConfig:
    env: str
    kafka: dict[str, Any]
    consumer: dict[str, Any]
    message: dict[str, Any]
    elasticsearch: dict[str, Any]


def load_config(env: str, config_dir: Path | None = None) -> AppConfig:
    base_dir = config_dir or CONFIG_DIR
    config_path = base_dir / f"{env}.toml"
    if not config_path.exists():
        available = ", ".join(sorted(path.stem for path in base_dir.glob("*.toml")))
        raise FileNotFoundError(
            f"Config file not found for env '{env}': {config_path}. "
            f"Available envs: {available or 'none'}"
        )

    with config_path.open("rb") as handle:
        raw = tomllib.load(handle)

    return AppConfig(
        env=raw.get("pipeline", {}).get("environment", env),
        kafka=dict(raw.get("kafka", {})),
        consumer=dict(raw.get("consumer", {})),
        message=dict(raw.get("message", {})),
        elasticsearch=dict(raw.get("elasticsearch", {})),
    )


def resolve_project_path(path_value: str | Path) -> Path:
    path = Path(path_value)
    if path.is_absolute():
        return path
    return PROJECT_ROOT / path
```

Line by line:

- `from __future__ import annotations` lets type hints be evaluated lazily. That keeps annotations lightweight and makes `Path | None` style hints work consistently.
- `dataclass` creates a small immutable config object without writing a manual class.
- `Path` gives path operations that work across operating systems.
- `tomllib` reads TOML files. It is built into Python 3.11.
- `Any` is used because config values can be strings, lists, booleans, and numbers.
- `PROJECT_ROOT = Path(__file__).resolve().parents[2]` finds the repository root from `src/etl/config.py`. `parents[0]` is `src/etl`, `parents[1]` is `src`, and `parents[2]` is the project root.
- `CONFIG_DIR = PROJECT_ROOT / "config"` creates the default config directory path.
- `@dataclass(frozen=True)` makes `AppConfig` immutable after creation.
- `env`, `kafka`, `consumer`, `message`, and `elasticsearch` mirror the TOML sections.
- `load_config` accepts an environment name such as `local`, `dev`, or `prod`.
- `config_dir` is optional so tests can pass a temporary config directory.
- `base_dir = config_dir or CONFIG_DIR` chooses the supplied test directory or the real config directory.
- `config_path = base_dir / f"{env}.toml"` builds the file name.
- `if not config_path.exists()` gives a useful error instead of a vague file-open failure.
- `available = ...` lists known environment files for the error message.
- `raise FileNotFoundError(...)` stops execution if the requested environment is missing.
- `with config_path.open("rb")` opens TOML in binary mode because `tomllib` expects bytes.
- `raw = tomllib.load(handle)` parses the TOML into nested dictionaries.
- `return AppConfig(...)` normalizes missing sections to empty dictionaries.
- `raw.get("pipeline", {}).get("environment", env)` uses the config environment name when present, otherwise the requested name.
- `dict(...)` creates normal mutable dictionaries for each section while the outer dataclass remains frozen.
- `resolve_project_path` lets config paths be either absolute or relative to the project root.
- `Path(path_value)` normalizes strings and `Path` objects.
- `if path.is_absolute()` preserves absolute paths.
- `return PROJECT_ROOT / path` makes relative config values predictable.

Checkpoint:

```bash
python - <<'PY'
from etl.config import load_config
config = load_config("local")
assert config.kafka["topic"] == "control-topic"
assert config.elasticsearch["index"] == "source-index"
print("config ok")
PY
```

## 5. Add JSON File Helpers

Create `src/etl/json_io.py`:

```python
from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def load_json_file(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        value = json.load(handle)
    if not isinstance(value, dict):
        raise ValueError(f"Expected JSON object in {path}")
    return value


def parse_json_object(raw_json: str) -> dict[str, Any]:
    value = json.loads(raw_json)
    if not isinstance(value, dict):
        raise ValueError("Expected a JSON object")
    return value


def write_json_file(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")
```

Line by line:

- `json` is the standard library module for reading and writing JSON.
- `Path` keeps file-path handling consistent with the config module.
- `Any` lets JSON dictionaries hold mixed values.
- `load_json_file` accepts a path and returns a JSON object.
- `path.open("r", encoding="utf-8")` opens text as UTF-8, which is the safest default for JSON.
- `json.load(handle)` parses the file.
- `if not isinstance(value, dict)` enforces that our pipeline messages are JSON objects, not arrays or strings.
- `raise ValueError(...)` gives a clear message when the JSON shape is wrong.
- `return value` returns the typed dictionary.
- `parse_json_object` does the same validation for a JSON string passed on the command line.
- `json.loads(raw_json)` parses the raw string.
- `write_json_file` saves a dictionary to disk.
- `path.parent.mkdir(parents=True, exist_ok=True)` creates the output directory if it does not exist.
- `path.open("w", encoding="utf-8")` writes UTF-8 text.
- `json.dump(..., indent=2, sort_keys=True)` produces readable, stable JSON files.
- `handle.write("\n")` ends the file with a newline, which is friendly to terminals and diffs.

Suggested test while rebuilding:

```python
from etl.json_io import load_json_file, parse_json_object, write_json_file


def test_json_round_trip(tmp_path):
    path = tmp_path / "message.json"
    write_json_file(path, {"event_id": "abc-123"})

    assert load_json_file(path) == {"event_id": "abc-123"}
    assert parse_json_object('{"event_id":"abc-123"}') == {"event_id": "abc-123"}
```

Run:

```bash
python -m pytest
```

## 6. Convert A Control Message

This is the first real ETL behavior. A Kafka control message might look like:

```json
{
  "event_id": "customer-123",
  "event_type": "customer.updated",
  "source_index": "source-index",
  "published_at": "2026-06-21T00:00:00Z",
  "attributes": {
    "tenant": "demo",
    "priority": "normal"
  }
}
```

Save that as `samples/control_message.json`.

Create `src/etl/message.py`:

```python
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import uuid4


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def convert_control_message(
    payload: dict[str, Any],
    *,
    id_attribute: str,
    source: str,
    kafka_metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if id_attribute not in payload:
        raise KeyError(f"Control message is missing required ID attribute '{id_attribute}'")

    return {
        "message_id": str(uuid4()),
        "converted_at": utc_now_iso(),
        "source": source,
        "id_attribute": id_attribute,
        "id_value": payload[id_attribute],
        "kafka": kafka_metadata or {},
        "payload": payload,
    }
```

Line by line:

- `datetime` and `timezone` are used to stamp the converted message in UTC.
- `Any` supports flexible JSON payload values.
- `uuid4` creates a unique file/message identifier.
- `utc_now_iso` returns the current UTC timestamp.
- `datetime.now(timezone.utc)` avoids local timezone ambiguity.
- `.isoformat()` produces a machine-readable timestamp.
- `.replace("+00:00", "Z")` uses the common UTC `Z` suffix.
- `convert_control_message` transforms one raw control payload into the canonical format used by the next component.
- `payload` is the original Kafka JSON object.
- `*` forces later arguments to be passed by keyword, making call sites easier to read.
- `id_attribute` is configurable because real systems may use `event_id`, `id`, `customer_id`, or another field.
- `source` records where this message came from, such as `kafka`, `manual-file`, or `test`.
- `kafka_metadata` is optional because manual debug runs do not have Kafka topic/partition/offset data.
- `if id_attribute not in payload` protects the ES lookup step from receiving a message without an ID.
- `raise KeyError(...)` fails fast with the missing field name.
- `message_id` is a generated unique ID for this converted message.
- `converted_at` records conversion time.
- `source` is copied into the output.
- `id_attribute` records which field was used.
- `id_value` promotes the ID to a stable top-level field.
- `kafka` stores Kafka metadata, or `{}` when none exists.
- `payload` preserves the full original control message.

Create `tests/test_message.py`:

```python
from etl.message import convert_control_message


def test_convert_control_message_promotes_configured_id_attribute():
    converted = convert_control_message(
        {"event_id": "abc-123", "name": "Example"},
        id_attribute="event_id",
        source="test",
    )

    assert converted["id_value"] == "abc-123"
    assert converted["payload"]["name"] == "Example"
```

Line by line:

- Import only the function under test.
- The test name describes behavior, not implementation.
- The sample input contains the configured ID plus another field.
- `id_attribute="event_id"` checks that the converter uses the configured field name.
- `source="test"` avoids pretending the message came from Kafka.
- The first assertion proves the ID was promoted.
- The second assertion proves the original payload was preserved.

Run:

```bash
python -m pytest tests/test_message.py
```

## 7. Build The Elasticsearch Lookup MVP

The lookup component should be independently runnable. Start with pure functions before calling Elasticsearch.

Create `src/etl/es_lookup.py`:

```python
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from .config import load_config
from .json_io import load_json_file, parse_json_object
```

Line by line:

- `argparse` builds the command-line interface.
- `json` prints structured command output.
- `Path` handles `--message-file`.
- `Any` supports flexible JSON query dictionaries.
- `load_config` reads the environment-specific ES settings.
- `load_json_file` reads converted message files.
- `parse_json_object` parses command-line JSON strings.

Add query helpers:

```python
def build_query_from_id(id_attribute: str, id_value: str) -> dict[str, Any]:
    return {"query": {"term": {id_attribute: id_value}}}


def normalize_query(query_json: dict[str, Any]) -> dict[str, Any]:
    if "query" in query_json:
        return query_json
    return {"query": query_json}
```

Line by line:

- `build_query_from_id` creates an Elasticsearch query body from a field and value.
- `{"query": {"term": ...}}` searches for an exact field value. This works best when the field is mapped as `keyword`.
- `normalize_query` accepts either a full ES query body or just a query clause.
- `if "query" in query_json` detects a full body.
- `return query_json` preserves full bodies unchanged.
- `return {"query": query_json}` wraps a raw clause like `{"term": {"event_id": "abc"}}`.

Add ID extraction:

```python
def extract_id_from_message(message: dict[str, Any], id_attribute: str) -> str:
    if "id_value" in message:
        return str(message["id_value"])

    payload = message.get("payload")
    if isinstance(payload, dict) and id_attribute in payload:
        return str(payload[id_attribute])

    if id_attribute in message:
        return str(message[id_attribute])

    raise KeyError(f"Could not find ID attribute '{id_attribute}' in message")
```

Line by line:

- `extract_id_from_message` supports multiple input shapes.
- If the input is already converted, `id_value` is the preferred source.
- `str(...)` normalizes IDs so the ES query builder receives a string.
- `payload = message.get("payload")` checks converted-message shape.
- `isinstance(payload, dict)` avoids errors if `payload` is missing or malformed.
- `id_attribute in payload` supports converted messages that only kept the ID in the original payload.
- `if id_attribute in message` supports raw control messages.
- The final `KeyError` tells the caller exactly what field was missing.

Add the live ES client:

```python
def create_es_client(es_config: dict[str, Any]):
    try:
        from elasticsearch import Elasticsearch
    except ImportError as exc:
        raise RuntimeError(
            "The 'elasticsearch' package is required for live queries. "
            "Install dependencies with 'pip install -e .' or run with --dry-run."
        ) from exc

    kwargs: dict[str, Any] = {
        "hosts": es_config.get("hosts", ["http://localhost:9200"]),
        "request_timeout": es_config.get("request_timeout_seconds", 30),
        "verify_certs": es_config.get("verify_certs", True),
    }
    if es_config.get("api_key"):
        kwargs["api_key"] = es_config["api_key"]
    elif es_config.get("username") and es_config.get("password"):
        kwargs["basic_auth"] = (es_config["username"], es_config["password"])
    return Elasticsearch(**kwargs)
```

Line by line:

- Importing `Elasticsearch` inside the function keeps dry-run mode usable even if the dependency is missing.
- The `RuntimeError` gives the developer a helpful install hint.
- `kwargs` gathers constructor arguments.
- `hosts` defaults to local ES.
- `request_timeout` is read from config.
- `verify_certs` is configurable for local HTTP versus production HTTPS.
- `if es_config.get("api_key")` prefers API-key auth when supplied.
- `elif username and password` falls back to basic auth.
- `return Elasticsearch(**kwargs)` creates the client.

Add the orchestration function:

```python
def run_lookup(
    *,
    env: str,
    message_file: str | None = None,
    raw_json: str | None = None,
    query_json: str | None = None,
    direct_id: str | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    config = load_config(env)
    id_attribute = str(config.message.get("id_attribute", "event_id"))
    index = str(config.elasticsearch.get("index", "source-index"))

    if query_json:
        query = normalize_query(parse_json_object(query_json))
    else:
        if direct_id is not None:
            id_value = direct_id
        elif message_file:
            id_value = extract_id_from_message(load_json_file(Path(message_file)), id_attribute)
        elif raw_json:
            id_value = extract_id_from_message(parse_json_object(raw_json), id_attribute)
        else:
            raise ValueError("Provide one of --message-file, --json, --query-json, or --id")
        query = build_query_from_id(id_attribute, id_value)

    if dry_run:
        return {"dry_run": True, "index": index, "query": query}

    client = create_es_client(config.elasticsearch)
    response = client.search(index=index, body=query)
    return {
        "dry_run": False,
        "index": index,
        "query": query,
        "response": response.body if hasattr(response, "body") else response,
    }
```

Line by line:

- The leading `*` makes every argument keyword-only. This prevents confusing positional calls.
- `env` selects `config/{env}.toml`.
- `message_file`, `raw_json`, `query_json`, and `direct_id` are the four supported manual input modes.
- `dry_run` lets developers inspect the ES query without connecting to ES.
- `config = load_config(env)` reads the selected environment.
- `id_attribute` comes from config, defaulting to `event_id`.
- `index` comes from config, defaulting to `source-index`.
- `if query_json` handles callers that already know the exact ES query they want.
- `normalize_query(parse_json_object(query_json))` parses and wraps the query if needed.
- The `else` block handles ID-based lookup modes.
- `direct_id` is simplest and wins first.
- `message_file` reads a converted or raw message from disk.
- `raw_json` parses a command-line JSON object.
- The `ValueError` prevents an empty lookup.
- `query = build_query_from_id(...)` turns the ID into an ES term query.
- `if dry_run` returns the query instead of calling ES.
- `client = create_es_client(...)` creates a live client only when needed.
- `client.search(index=index, body=query)` performs the ES search.
- The returned dictionary includes the dry-run flag, index, query, and live response.
- `response.body if hasattr(...)` supports newer Elasticsearch client response objects while still tolerating plain dictionaries.

Add the CLI:

```python
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Query Elasticsearch from a Kafka control JSON message.")
    parser.add_argument("--env", default="local", help="Config environment name, e.g. local/dev/prod.")
    parser.add_argument("--message-file", help="Converted Kafka JSON file to read.")
    parser.add_argument("--json", dest="raw_json", help="Raw JSON object containing the configured ID attribute.")
    parser.add_argument("--query-json", help="Elasticsearch query JSON. May be a query body or just the query clause.")
    parser.add_argument("--id", dest="direct_id", help="Direct ID value to search for.")
    parser.add_argument("--dry-run", action="store_true", help="Print the query without calling Elasticsearch.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = run_lookup(
        env=args.env,
        message_file=args.message_file,
        raw_json=args.raw_json,
        query_json=args.query_json,
        direct_id=args.direct_id,
        dry_run=args.dry_run,
    )
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
```

Line by line:

- `parse_args` isolates command-line parsing from business logic.
- `ArgumentParser(...)` creates a CLI with help text.
- `--env` defaults to `local`.
- `--message-file` accepts a saved converted Kafka message.
- `--json` accepts raw JSON from the shell. `dest="raw_json"` avoids using `json` as a Python variable name.
- `--query-json` accepts direct ES query JSON.
- `--id` accepts a direct ID and stores it as `direct_id`.
- `--dry-run` is a boolean flag.
- `main` reads args, calls `run_lookup`, and prints JSON.
- `json.dumps(..., indent=2, sort_keys=True)` makes CLI output stable and readable.
- `if __name__ == "__main__"` lets the module run via `python -m etl.es_lookup`.

Create `tests/test_es_lookup.py`:

```python
from etl.es_lookup import build_query_from_id, extract_id_from_message, normalize_query


def test_build_query_from_id():
    assert build_query_from_id("event_id", "abc-123") == {
        "query": {"term": {"event_id": "abc-123"}}
    }


def test_extract_id_from_converted_message():
    assert extract_id_from_message({"id_value": "abc-123"}, "event_id") == "abc-123"


def test_normalize_query_wraps_clause():
    assert normalize_query({"term": {"event_id": "abc-123"}}) == {
        "query": {"term": {"event_id": "abc-123"}}
    }
```

Run:

```bash
python -m pytest tests/test_es_lookup.py
python -m etl.es_lookup --env local --id customer-123 --dry-run
```

The dry-run output should include:

```json
{
  "dry_run": true,
  "index": "source-index",
  "query": {
    "query": {
      "term": {
        "event_id": "customer-123"
      }
    }
  }
}
```

## 8. Build The Kafka Consumer MVP

The consumer has two modes:

- Live Kafka mode.
- Manual debug mode using `--message-file`.

Manual debug mode is important because it lets a developer test conversion and chaining without Kafka running.

Create `src/etl/kafka_consumer.py` and start with imports:

```python
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Iterable

from .config import load_config, resolve_project_path
from .json_io import load_json_file, write_json_file
from .message import convert_control_message
```

Line by line:

- `argparse` builds the CLI.
- `json` decodes Kafka message bytes and prints results.
- `subprocess` triggers the next component.
- `sys` provides the current Python executable and stdout flushing.
- `time` enforces the delay between non-empty Kafka batches.
- `Path` handles debug files.
- `Any` and `Iterable` type the Kafka message helpers.
- `load_config` reads environment config.
- `resolve_project_path` turns config output paths into absolute paths.
- `load_json_file` supports manual debug input.
- `write_json_file` saves converted messages.
- `convert_control_message` performs the core transformation.

Add batching defaults:

```python
DEFAULT_BATCH_INTERVAL_SECONDS = 600.0
DEFAULT_BATCH_MAX_RECORDS = 100
DEFAULT_POLL_TIMEOUT_MS = 1000
```

Line by line:

- `DEFAULT_BATCH_INTERVAL_SECONDS = 600.0` means the continuous consumer processes at most one non-empty batch every ten minutes.
- `DEFAULT_BATCH_MAX_RECORDS = 100` caps how many Kafka records can be processed in one batch.
- `DEFAULT_POLL_TIMEOUT_MS = 1000` keeps empty polls responsive without spinning tightly.

Add Kafka value decoding:

```python
def decode_kafka_value(value: bytes | str | dict[str, Any]) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, bytes):
        value = value.decode("utf-8")
    parsed = json.loads(value)
    if not isinstance(parsed, dict):
        raise ValueError("Kafka message value must decode to a JSON object")
    return parsed
```

Line by line:

- Kafka values normally arrive as bytes, but tests or serializers might pass strings or dictionaries.
- If the value is already a dictionary, return it unchanged.
- If the value is bytes, decode it as UTF-8 JSON text.
- `json.loads(value)` parses the JSON string.
- The shape check rejects arrays and scalar values.
- Return the parsed JSON object.

Add the live Kafka consumer factory:

```python
def create_kafka_consumer(kafka_config: dict[str, Any]):
    try:
        from kafka import KafkaConsumer
    except ImportError as exc:
        raise RuntimeError(
            "The 'kafka-python' package is required for live Kafka consumption. "
            "Install dependencies with 'pip install -e .' or use --message-file for debug runs."
        ) from exc

    topic = kafka_config["topic"]
    return KafkaConsumer(
        topic,
        bootstrap_servers=kafka_config.get("bootstrap_servers", ["localhost:9092"]),
        group_id=kafka_config.get("group_id"),
        client_id=kafka_config.get("client_id", "etl-control-consumer"),
        auto_offset_reset=kafka_config.get("auto_offset_reset", "latest"),
        enable_auto_commit=kafka_config.get("enable_auto_commit", True),
        security_protocol=kafka_config.get("security_protocol", "PLAINTEXT"),
        max_poll_interval_ms=kafka_config.get("max_poll_interval_ms", 900000),
        value_deserializer=lambda value: decode_kafka_value(value),
    )
```

Line by line:

- Importing `KafkaConsumer` inside the function keeps manual debug mode usable if Kafka dependencies are missing.
- The error message explains how to install dependencies or avoid live Kafka.
- `topic = kafka_config["topic"]` requires the config to specify a topic.
- `KafkaConsumer(...)` subscribes to that topic.
- `bootstrap_servers` points to Kafka brokers.
- `group_id` controls committed offsets.
- `client_id` labels the client.
- `auto_offset_reset` decides where to start when there is no committed offset.
- `enable_auto_commit` controls automatic offset commits.
- `security_protocol` supports local plaintext and production SSL/SASL settings.
- `max_poll_interval_ms` is set higher than ten minutes so Kafka does not remove the consumer from the group while it waits between batches.
- `value_deserializer` turns Kafka bytes into dictionaries immediately.

Add metadata helpers:

```python
def message_metadata(kafka_message: Any) -> dict[str, Any]:
    return {
        "topic": getattr(kafka_message, "topic", None),
        "partition": getattr(kafka_message, "partition", None),
        "offset": getattr(kafka_message, "offset", None),
        "timestamp": getattr(kafka_message, "timestamp", None),
        "key": _decode_key(getattr(kafka_message, "key", None)),
    }


def _decode_key(key: Any) -> str | None:
    if key is None:
        return None
    if isinstance(key, bytes):
        return key.decode("utf-8", errors="replace")
    return str(key)
```

Line by line:

- `message_metadata` extracts operational Kafka details from the consumed message.
- `getattr(..., None)` avoids crashing if a test double lacks one field.
- `topic`, `partition`, and `offset` identify where the message came from.
- `timestamp` records the Kafka timestamp.
- `key` is decoded through `_decode_key`.
- `_decode_key` returns `None` when Kafka has no key.
- Bytes keys are decoded as UTF-8.
- `errors="replace"` avoids crashing on unusual key bytes.
- Non-bytes keys are converted to strings.

Add output file naming:

```python
def output_path(output_dir: Path, converted_message: dict[str, Any]) -> Path:
    message_id = str(converted_message["message_id"])
    id_value = str(converted_message["id_value"]).replace("/", "_")
    return output_dir / f"{id_value}-{message_id}.json"
```

Line by line:

- `message_id` guarantees uniqueness.
- `id_value` makes the filename recognizable.
- `.replace("/", "_")` prevents IDs with slashes from creating nested directories.
- The filename combines business ID and generated message ID.

Add the next-component trigger:

```python
def trigger_next_component(
    *,
    command: list[str],
    env: str,
    message_file: Path,
    dry_run_next: bool,
) -> subprocess.CompletedProcess[str]:
    if command and command[0] == "python":
        command = [sys.executable, *command[1:]]
    full_command = [*command, "--env", env, "--message-file", str(message_file)]
    if dry_run_next:
        full_command.append("--dry-run")
    return subprocess.run(full_command, check=True, text=True, capture_output=True)
```

Line by line:

- The function is keyword-only to make call sites explicit.
- `command` comes from config, usually `["python", "-m", "etl.es_lookup"]`.
- `env` is passed through so both components use the same config.
- `message_file` tells the lookup component which converted JSON file to read.
- `dry_run_next` lets bootstrap scripts test chaining without live ES.
- If the config command starts with `python`, replace it with `sys.executable`.
- That ensures the triggered component runs inside the same venv as the consumer.
- `full_command` appends the environment and message-file arguments.
- If `dry_run_next` is true, append `--dry-run`.
- `subprocess.run(..., check=True)` raises if the next script fails.
- `text=True` returns stdout/stderr as strings.
- `capture_output=True` stores stdout/stderr so the consumer can include them in its JSON output.

Add batch helpers:

```python
def flatten_polled_records(polled_records: dict[Any, list[Any]] | None) -> list[Any]:
    if not polled_records:
        return []

    messages: list[Any] = []
    for partition_records in polled_records.values():
        messages.extend(partition_records)
    return messages


def seconds_until_next_batch(batch_started_at: float, batch_interval_seconds: float) -> float:
    if batch_interval_seconds <= 0:
        return 0.0
    elapsed_seconds = time.monotonic() - batch_started_at
    return max(0.0, batch_interval_seconds - elapsed_seconds)


def resolve_batch_options(
    consumer_config: dict[str, Any],
    *,
    batch_max_records: int | None,
    batch_interval_seconds: float | None,
    poll_timeout_ms: int | None,
) -> tuple[int, float, int]:
    resolved_max_records = int(
        batch_max_records
        if batch_max_records is not None
        else consumer_config.get("batch_max_records", DEFAULT_BATCH_MAX_RECORDS)
    )
    resolved_interval_seconds = float(
        batch_interval_seconds
        if batch_interval_seconds is not None
        else consumer_config.get("batch_interval_seconds", DEFAULT_BATCH_INTERVAL_SECONDS)
    )
    resolved_poll_timeout_ms = int(
        poll_timeout_ms
        if poll_timeout_ms is not None
        else consumer_config.get("poll_timeout_ms", DEFAULT_POLL_TIMEOUT_MS)
    )

    if resolved_max_records <= 0:
        raise ValueError("batch_max_records must be greater than 0")
    if resolved_interval_seconds < 0:
        raise ValueError("batch_interval_seconds cannot be negative")
    if resolved_poll_timeout_ms <= 0:
        raise ValueError("poll_timeout_ms must be greater than 0")

    return resolved_max_records, resolved_interval_seconds, resolved_poll_timeout_ms
```

Line by line:

- `flatten_polled_records` turns Kafka's partition-grouped `poll()` result into a single list of messages.
- Empty polls return an empty list.
- `seconds_until_next_batch` computes how much of the configured interval remains after processing a batch.
- `time.monotonic()` is used for elapsed time because it is stable even if the system clock changes.
- `max(0.0, ...)` prevents negative sleep times when processing takes longer than the interval.
- `resolve_batch_options` merges config values with optional CLI overrides.
- The validations prevent accidental zero-record batches, negative intervals, and zero-timeout busy loops.

Add the core processing function:

```python
def process_payload(
    *,
    payload: dict[str, Any],
    env: str,
    source: str,
    kafka_metadata: dict[str, Any] | None,
    trigger_next: bool | None,
    dry_run_next: bool,
) -> dict[str, Any]:
    config = load_config(env)
    id_attribute = str(config.message.get("id_attribute", "event_id"))
    converted = convert_control_message(
        payload,
        id_attribute=id_attribute,
        source=source,
        kafka_metadata=kafka_metadata,
    )

    destination = output_path(resolve_project_path(config.consumer.get("output_dir", "data/converted")), converted)
    write_json_file(destination, converted)

    should_trigger = bool(config.consumer.get("trigger_next", False)) if trigger_next is None else trigger_next
    next_result: dict[str, Any] | None = None
    if should_trigger:
        completed = trigger_next_component(
            command=list(config.consumer.get("next_command", ["python", "-m", "etl.es_lookup"])),
            env=env,
            message_file=destination,
            dry_run_next=dry_run_next,
        )
        next_result = {
            "returncode": completed.returncode,
            "stdout": completed.stdout.strip(),
            "stderr": completed.stderr.strip(),
        }

    return {"saved_file": str(destination), "converted": converted, "next_result": next_result}
```

Line by line:

- `process_payload` is the heart of the consumer. It works for both Kafka and manual file input.
- `payload` is the decoded control message.
- `env` selects config.
- `source` records where the message came from.
- `kafka_metadata` is present for Kafka mode and absent for manual mode.
- `trigger_next` can override config. `None` means use config.
- `dry_run_next` is forwarded to the ES lookup script.
- `config = load_config(env)` reads settings.
- `id_attribute` selects the ID field.
- `convert_control_message(...)` builds the canonical converted message.
- `destination = ...` computes the output file path.
- `resolve_project_path(...)` makes relative output dirs relative to the project root.
- `write_json_file(...)` persists the converted message.
- `should_trigger = ...` chooses either config behavior or CLI override.
- `next_result` starts as `None`, meaning no next script was run.
- If triggering is enabled, run the next command from config.
- `list(...)` copies the configured command so it can be safely modified.
- `next_result` captures return code, stdout, and stderr.
- The final return value includes the saved file, converted message, and optional next-script result.

Add live consumption:

```python
def consume_messages(
    *,
    env: str,
    once: bool,
    trigger_next: bool | None,
    dry_run_next: bool,
    batch_max_records: int | None = None,
    batch_interval_seconds: float | None = None,
    poll_timeout_ms: int | None = None,
) -> Iterable[dict[str, Any]]:
    config = load_config(env)
    resolved_max_records, resolved_interval_seconds, resolved_poll_timeout_ms = resolve_batch_options(
        config.consumer,
        batch_max_records=batch_max_records,
        batch_interval_seconds=batch_interval_seconds,
        poll_timeout_ms=poll_timeout_ms,
    )
    consumer = create_kafka_consumer(config.kafka)
    try:
        while True:
            batch_started_at = time.monotonic()
            kafka_messages = flatten_polled_records(
                consumer.poll(timeout_ms=resolved_poll_timeout_ms, max_records=resolved_max_records)
            )
            if not kafka_messages:
                continue

            for kafka_message in kafka_messages:
                result = process_payload(
                    payload=kafka_message.value,
                    env=env,
                    source="kafka",
                    kafka_metadata=message_metadata(kafka_message),
                    trigger_next=trigger_next,
                    dry_run_next=dry_run_next,
                )
                yield result

            if once:
                break

            sleep_seconds = seconds_until_next_batch(batch_started_at, resolved_interval_seconds)
            if sleep_seconds > 0:
                time.sleep(sleep_seconds)
    finally:
        consumer.close()
```

Line by line:

- `consume_messages` is a generator so callers can print each result as messages arrive.
- `once` supports integration tests and debug runs by processing one batch and exiting.
- The optional batch arguments let CLI flags override config.
- Load config and create a Kafka consumer.
- `resolve_batch_options(...)` combines config defaults and CLI overrides.
- `try/finally` guarantees the consumer closes.
- `while True` keeps the server running.
- `batch_started_at = time.monotonic()` records when the batch window began.
- `consumer.poll(...)` fetches up to the configured max records.
- Empty polls do not count as consumed batches, so the loop polls again.
- `kafka_message.value` is already decoded by the configured deserializer.
- `source="kafka"` marks live input.
- `message_metadata(...)` records topic, partition, offset, timestamp, and key.
- `yield result` returns one processed message to the CLI.
- `if once: break` exits after one batch when requested.
- `seconds_until_next_batch(...)` calculates the remaining wait time.
- `time.sleep(...)` prevents more than one non-empty batch from being consumed during the configured interval.
- `consumer.close()` releases network resources.

Add CLI parsing:

```python
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Consume Kafka control messages and save converted JSON.")
    parser.add_argument("--env", default="local", help="Config environment name, e.g. local/dev/prod.")
    parser.add_argument("--once", action="store_true", help="Consume one Kafka batch and exit.")
    parser.add_argument("--message-file", help="Debug mode: read this JSON file instead of Kafka.")
    parser.add_argument("--trigger-next", action="store_true", help="Trigger the Elasticsearch lookup component.")
    parser.add_argument("--no-trigger-next", action="store_true", help="Do not trigger the next component.")
    parser.add_argument("--dry-run-next", action="store_true", help="Pass --dry-run to the next component.")
    parser.add_argument("--batch-max-records", type=int, help="Maximum Kafka records to process in one batch.")
    parser.add_argument(
        "--batch-interval-seconds",
        type=float,
        help="Minimum seconds between non-empty Kafka batches. Defaults to 600.",
    )
    parser.add_argument("--poll-timeout-ms", type=int, help="Kafka poll timeout in milliseconds.")
    return parser.parse_args()
```

Line by line:

- `--env` chooses config.
- `--once` makes live Kafka mode exit after one batch.
- `--message-file` switches to manual debug mode.
- `--trigger-next` forces chaining on.
- `--no-trigger-next` forces chaining off.
- `--dry-run-next` makes the ES lookup print the query without connecting to ES.
- `--batch-max-records` overrides the configured batch size.
- `--batch-interval-seconds` overrides the configured wait between non-empty batches.
- `--poll-timeout-ms` overrides the Kafka poll timeout.

Add trigger override validation and `main`:

```python
def trigger_override(args: argparse.Namespace) -> bool | None:
    if args.trigger_next and args.no_trigger_next:
        raise ValueError("Use only one of --trigger-next or --no-trigger-next")
    if args.trigger_next:
        return True
    if args.no_trigger_next:
        return False
    return None


def main() -> None:
    args = parse_args()
    trigger_next = trigger_override(args)

    if args.message_file:
        result = process_payload(
            payload=load_json_file(Path(args.message_file)),
            env=args.env,
            source="manual-file",
            kafka_metadata=None,
            trigger_next=trigger_next,
            dry_run_next=args.dry_run_next,
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return

    for result in consume_messages(
        env=args.env,
        once=args.once,
        trigger_next=trigger_next,
        dry_run_next=args.dry_run_next,
        batch_max_records=args.batch_max_records,
        batch_interval_seconds=args.batch_interval_seconds,
        poll_timeout_ms=args.poll_timeout_ms,
    ):
        print(json.dumps(result, indent=2, sort_keys=True))
        sys.stdout.flush()


if __name__ == "__main__":
    main()
```

Line by line:

- `trigger_override` converts CLI flags into `True`, `False`, or `None`.
- If both trigger flags are supplied, raise an error.
- `True` means force trigger.
- `False` means force no trigger.
- `None` means use config.
- `main` parses CLI args.
- Manual-file mode calls `process_payload` directly.
- `source="manual-file"` makes the output honest about where the data came from.
- `kafka_metadata=None` because manual mode has no Kafka offset.
- Print the result and return so Kafka mode does not start.
- Live mode iterates over `consume_messages`.
- Print each result as formatted JSON.
- `sys.stdout.flush()` makes logs visible promptly in long-running server mode.
- The module guard enables `python -m etl.kafka_consumer`.

Manual checkpoint without Kafka:

```bash
python -m etl.kafka_consumer \
  --env local \
  --message-file samples/control_message.json \
  --trigger-next \
  --dry-run-next
```

Expected behavior:

- A new file appears under `data/converted/`.
- The output includes `source = "manual-file"`.
- The output includes a nested `next_result` from the ES lookup dry run.

## 9. Build The Kafka Producer For Tests

The producer is not one of the two main components. It exists so local integration tests can publish a message without using Kafka shell scripts.

Create `src/etl/kafka_producer.py`:

```python
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from .config import load_config
from .json_io import load_json_file, parse_json_object
```

Line by line:

- The imports mirror the other CLIs.
- `json` serializes the Kafka message value.
- `Path` supports `--message-file`.
- `load_config` reads Kafka connection settings.
- `load_json_file` and `parse_json_object` support file and inline JSON input.

Add the producer factory:

```python
def create_kafka_producer(kafka_config: dict[str, Any]):
    try:
        from kafka import KafkaProducer
    except ImportError as exc:
        raise RuntimeError(
            "The 'kafka-python' package is required to publish Kafka messages. "
            "Install dependencies with 'pip install -e .'."
        ) from exc

    return KafkaProducer(
        bootstrap_servers=kafka_config.get("bootstrap_servers", ["localhost:9092"]),
        client_id=f"{kafka_config.get('client_id', 'etl-control')}-producer",
        security_protocol=kafka_config.get("security_protocol", "PLAINTEXT"),
        key_serializer=lambda value: value.encode("utf-8") if value is not None else None,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )
```

Line by line:

- Lazy import keeps the module importable even before dependencies are installed.
- `bootstrap_servers` and `security_protocol` come from config.
- The producer client ID reuses the consumer client ID with `-producer`.
- `key_serializer` encodes optional string keys as UTF-8 bytes.
- `value_serializer` turns dictionaries into JSON bytes.

Add publishing:

```python
def publish_message(
    *,
    env: str,
    payload: dict[str, Any],
    key: str | None = None,
    topic: str | None = None,
) -> dict[str, Any]:
    config = load_config(env)
    selected_topic = topic or str(config.kafka["topic"])
    producer = create_kafka_producer(config.kafka)
    try:
        future = producer.send(selected_topic, key=key, value=payload)
        metadata = future.get(timeout=30)
        producer.flush(timeout=30)
        return {
            "topic": metadata.topic,
            "partition": metadata.partition,
            "offset": metadata.offset,
            "key": key,
            "payload": payload,
        }
    finally:
        producer.close()
```

Line by line:

- `env` chooses config.
- `payload` is the JSON object to publish.
- `key` is optional.
- `topic` can override config for ad hoc tests.
- `selected_topic` uses the override or configured topic.
- Create the Kafka producer.
- `try/finally` guarantees the producer closes.
- `producer.send(...)` sends the record asynchronously.
- `future.get(timeout=30)` waits for broker acknowledgement.
- `producer.flush(timeout=30)` ensures buffered records are sent.
- Return topic, partition, offset, key, and payload so tests and humans can see what happened.
- `producer.close()` releases resources.

Add CLI:

```python
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Publish a control message to Kafka.")
    parser.add_argument("--env", default="local", help="Config environment name, e.g. local/dev/prod.")
    parser.add_argument("--message-file", help="JSON message file to publish.")
    parser.add_argument("--json", dest="raw_json", help="Raw JSON object to publish.")
    parser.add_argument("--topic", help="Override Kafka topic from config.")
    parser.add_argument("--key", help="Optional Kafka message key.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.message_file:
        payload = load_json_file(Path(args.message_file))
    elif args.raw_json:
        payload = parse_json_object(args.raw_json)
    else:
        raise ValueError("Provide --message-file or --json")

    result = publish_message(env=args.env, payload=payload, key=args.key, topic=args.topic)
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
```

Line by line:

- The CLI supports file input and inline JSON.
- `--topic` and `--key` are useful in integration tests.
- `main` chooses which input mode to parse.
- If neither input mode is supplied, fail clearly.
- Publish the message and print broker metadata.

## 10. Add Bootstrap Scripts

Create `scripts/bootstrap_es_lookup.sh`:

```bash
#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
PYTHONPATH="${PYTHONPATH:-}:src" python -m etl.es_lookup \
  --env local \
  --json '{"event_id":"customer-123","event_type":"manual.debug"}' \
  --dry-run
```

Line by line:

- The shebang runs the script with Bash.
- `set -euo pipefail` makes shell failures visible.
- `cd "$(dirname "$0")/.."` moves to the project root no matter where the script was launched.
- `PYTHONPATH=...:src` lets the script run before editable install, though the venv install is preferred.
- `python -m etl.es_lookup` runs the ES lookup module.
- `--env local` uses local config.
- `--json ...` passes a raw control-like JSON object.
- `--dry-run` avoids live ES.

Create `scripts/bootstrap_consumer.sh`:

```bash
#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
PYTHONPATH="${PYTHONPATH:-}:src" python -m etl.kafka_consumer \
  --env local \
  --message-file samples/control_message.json \
  --trigger-next \
  --dry-run-next
```

Line by line:

- This script tests the consumer without Kafka.
- It reads `samples/control_message.json`.
- It forces `--trigger-next`.
- It passes `--dry-run-next` so the downstream ES lookup does not require Elasticsearch.

Make both executable:

```bash
chmod +x scripts/bootstrap_es_lookup.sh scripts/bootstrap_consumer.sh
```

Run:

```bash
./scripts/bootstrap_es_lookup.sh
./scripts/bootstrap_consumer.sh
```

## 11. Add Local Docker Kafka And Elasticsearch

Create `docker-compose.yml`:

```yaml
services:
  kafka:
    image: apache/kafka:3.9.1
    container_name: kafka-codex-kafka
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: "1"
      KAFKA_PROCESS_ROLES: "broker,controller"
      KAFKA_LISTENERS: "PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093"
      KAFKA_ADVERTISED_LISTENERS: "PLAINTEXT://localhost:9092"
      KAFKA_CONTROLLER_LISTENER_NAMES: "CONTROLLER"
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT"
      KAFKA_CONTROLLER_QUORUM_VOTERS: "1@localhost:9093"
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: "1"
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: "1"
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: "1"
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: "0"
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"

  elasticsearch:
    image: docker.elastic.co/elasticsearch/elasticsearch:8.19.3
    container_name: kafka-codex-elasticsearch
    ports:
      - "9200:9200"
    environment:
      discovery.type: "single-node"
      xpack.security.enabled: "false"
      ES_JAVA_OPTS: "-Xms512m -Xmx512m"
```

Line by line:

- `services` declares containers.
- `kafka` is a single-node Kafka service.
- `apache/kafka:3.9.1` gives a modern Kafka image that can run without ZooKeeper.
- `container_name` makes the container easy to inspect.
- `ports` publishes broker port `9092` to the host.
- `KAFKA_NODE_ID` identifies the broker/controller node.
- `KAFKA_PROCESS_ROLES` enables Kafka KRaft broker and controller roles.
- `KAFKA_LISTENERS` binds broker and controller listeners inside the container.
- `KAFKA_ADVERTISED_LISTENERS` tells host clients to connect to `localhost:9092`.
- `KAFKA_CONTROLLER_LISTENER_NAMES` selects the controller listener.
- `KAFKA_LISTENER_SECURITY_PROTOCOL_MAP` maps both listeners to plaintext.
- `KAFKA_CONTROLLER_QUORUM_VOTERS` configures the single-node controller quorum.
- Replication factor settings are `1` because this is a single broker.
- `KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS = 0` makes local tests faster.
- `KAFKA_AUTO_CREATE_TOPICS_ENABLE = true` lets the test topic be created automatically.
- `elasticsearch` is a single-node ES service.
- `ports` publishes `9200` to the host.
- `discovery.type = single-node` disables cluster discovery.
- `xpack.security.enabled = false` makes local HTTP unauthenticated.
- `ES_JAVA_OPTS` keeps memory use modest for local development.

Start services:

```bash
docker compose up -d kafka elasticsearch
```

Check:

```bash
docker ps --filter name=kafka-codex
curl http://localhost:9200
```

## 12. Add The End-To-End Docker Bootstrap

Create `scripts/bootstrap_e2e_docker.sh`:

```bash
#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

PYTHON="${PYTHON:-.venv/bin/python}"
EVENT_ID="${EVENT_ID:-customer-123}"

for _ in {1..60}; do
  if curl -fsS "http://localhost:9200" >/dev/null; then
    break
  fi
  sleep 2
done

curl -fsS -X PUT "http://localhost:9200/source-index" \
  -H "Content-Type: application/json" \
  -d '{"mappings":{"properties":{"event_id":{"type":"keyword"},"name":{"type":"text"},"updated_at":{"type":"date"}}}}' >/dev/null || true

curl -fsS -X POST "http://localhost:9200/source-index/_doc/${EVENT_ID}?refresh=true" \
  -H "Content-Type: application/json" \
  -d "{\"event_id\":\"${EVENT_ID}\",\"name\":\"Demo Customer\",\"updated_at\":\"2026-06-21T00:00:00Z\"}" >/dev/null

"${PYTHON}" -m etl.kafka_producer \
  --env local \
  --message-file samples/control_message.json \
  --key "${EVENT_ID}"

"${PYTHON}" -m etl.kafka_consumer \
  --env local \
  --once \
  --trigger-next
```

Line by line:

- The script runs from the project root.
- `PYTHON` defaults to `.venv/bin/python` but can be overridden by the caller.
- `EVENT_ID` defaults to `customer-123`.
- The `for` loop waits up to 120 seconds for Elasticsearch to accept HTTP.
- `curl -fsS` fails on HTTP errors, stays quiet on success, and prints errors.
- The first ES `PUT` creates the index and maps `event_id` as `keyword`.
- `|| true` allows the script to continue if the index already exists.
- The ES `POST` writes a test document whose ID matches the Kafka control message.
- `refresh=true` makes the document searchable immediately.
- The producer publishes the sample control message to Kafka.
- The consumer reads exactly one Kafka message.
- `--trigger-next` makes the consumer run the ES lookup.

Make executable:

```bash
chmod +x scripts/bootstrap_e2e_docker.sh
```

Run:

```bash
docker compose up -d kafka elasticsearch
./scripts/bootstrap_e2e_docker.sh
```

Expected output:

- The producer prints Kafka topic, partition, and offset.
- The consumer prints the converted message and saved file path.
- `next_result.stdout` contains an ES response with one hit for `customer-123`.

## 13. The Main Developer Workflows

Run all unit tests:

```bash
.venv/bin/python -m pytest
```

Run ES lookup without live ES:

```bash
.venv/bin/python -m etl.es_lookup --env local --id customer-123 --dry-run
```

Run ES lookup against live ES:

```bash
.venv/bin/python -m etl.es_lookup --env local --id customer-123
```

Run consumer without Kafka:

```bash
.venv/bin/python -m etl.kafka_consumer \
  --env local \
  --message-file samples/control_message.json \
  --trigger-next \
  --dry-run-next
```

Publish a Kafka message:

```bash
.venv/bin/python -m etl.kafka_producer \
  --env local \
  --message-file samples/control_message.json \
  --key customer-123
```

Consume one live Kafka message and trigger ES:

```bash
.venv/bin/python -m etl.kafka_consumer --env local --once --trigger-next
```

Run full local Docker integration:

```bash
docker compose up -d kafka elasticsearch
./scripts/bootstrap_e2e_docker.sh
```

## 14. How The Pieces Fit Together

The live flow is:

```text
Kafka topic
  -> etl.kafka_consumer
  -> convert_control_message
  -> data/converted/<id>-<uuid>.json
  -> etl.es_lookup --message-file <converted file>
  -> Elasticsearch search
```

The manual debug flow is:

```text
samples/control_message.json
  -> etl.kafka_consumer --message-file
  -> data/converted/<id>-<uuid>.json
  -> optional dry-run or live etl.es_lookup
```

The ES lookup input modes are:

```text
--id customer-123
--json '{"event_id":"customer-123"}'
--message-file data/converted/customer-123-....json
--query-json '{"term":{"event_id":"customer-123"}}'
```

## 15. Where To Change Config For Your Own Services

Use the config file that matches your target environment:

- `config/local.toml` for local Docker.
- `config/dev.toml` for development services.
- `config/prod.toml` for production services.

For Kafka, change:

```toml
[kafka]
bootstrap_servers = ["your-kafka-host:9092"]
topic = "your-control-topic"
group_id = "your-consumer-group"
client_id = "your-client-id"
auto_offset_reset = "latest"
enable_auto_commit = true
security_protocol = "PLAINTEXT"
```

For the message shape, change:

```toml
[message]
id_attribute = "event_id"
```

For Elasticsearch, change:

```toml
[elasticsearch]
hosts = ["http://your-es-host:9200"]
index = "your-index-name"
request_timeout_seconds = 30
verify_certs = true
username = ""
password = ""
api_key = ""
```

If your Kafka or ES service uses SASL, SSL certificates, cloud IDs, or custom headers, the next code change should be to expand `create_kafka_consumer`, `create_kafka_producer`, and `create_es_client` to pass those extra config fields into the client constructors.

## 16. Recommended Rebuild Order

Use this order when recreating the codebase:

1. `pyproject.toml`, `.gitignore`, package folders.
2. `config/*.toml`.
3. `src/etl/config.py`.
4. `src/etl/json_io.py`.
5. `src/etl/message.py` plus `tests/test_message.py`.
6. `src/etl/es_lookup.py` plus `tests/test_es_lookup.py`.
7. `samples/control_message.json`.
8. `src/etl/kafka_consumer.py`.
9. `scripts/bootstrap_es_lookup.sh` and `scripts/bootstrap_consumer.sh`.
10. `src/etl/kafka_producer.py`.
11. `docker-compose.yml`.
12. `scripts/bootstrap_e2e_docker.sh`.
13. Run unit tests.
14. Run dry-run bootstraps.
15. Run Docker e2e.

That order keeps each step testable. The project grows from pure functions, to file IO, to CLIs, to external services.

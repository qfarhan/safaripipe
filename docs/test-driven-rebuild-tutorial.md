# Build Kafka Codex ETL From Scratch — A Test-Driven Walkthrough

This tutorial rebuilds the entire `kafka-codex` project from an empty directory,
**one verifiable checkpoint at a time**. The rule we follow throughout:

> Write the smallest useful piece → write a test or run a live command that proves
> it works → only then move to the next piece.

By the end you will have rebuilt every line of the codebase, understood *why* each
line exists, and verified the whole thing live against real Kafka and Elasticsearch
containers.

There is a companion document, [`rebuild-from-scratch-tutorial.md`](rebuild-from-scratch-tutorial.md),
which is a straight structural walkthrough. This one is different: it is organized
around **checkpoints**. Every section ends with a command you actually run, and the
real output you should expect. If a checkpoint fails, you do not move on.

---

## What we are building

Two small, independent Python components plus a test producer:

```
                 ┌──────────────────────┐
  Kafka topic →  │  etl.kafka_consumer  │ → writes canonical JSON to data/converted/
  "control-topic"│  (poll → convert →   │        │
                 │   save → trigger)    │        │ (optionally triggers, as a subprocess)
                 └──────────────────────┘        ▼
                                          ┌──────────────────┐
                                          │  etl.es_lookup   │ → queries Elasticsearch
                                          │ (resolve id →    │     index, returns hits
                                          │  build query)    │
                                          └──────────────────┘

  etl.kafka_producer  →  publishes test messages onto the topic (used only by the e2e test)
```

Design principles that make this codebase easy to test — keep them in mind, because
every stage is shaped by them:

1. **Pure logic is separated from I/O.** Functions like `convert_control_message`,
   `build_query_from_id`, `flatten_polled_records`, and `resolve_batch_options` take
   plain data and return plain data. They need no broker, no cluster, no network — so
   they are trivially unit-testable.
2. **Network clients are imported lazily**, inside factory functions. The `kafka` and
   `elasticsearch` libraries are only imported the moment you actually create a client.
   That means every debug/dry-run path runs with zero infrastructure.
3. **Configuration is per-environment TOML** with safe fallbacks, selected by a single
   `--env` flag.

---

## Target project shape

```text
kafka-codex/
  config/
    local.toml          # points at localhost Kafka + ES (for Docker dev)
    dev.toml            # placeholder dev cluster
    prod.toml           # placeholder prod cluster (SASL_SSL + https)
  data/converted/       # output dir for converted messages (created at runtime)
  docs/
    test-driven-rebuild-tutorial.md
  samples/
    control_message.json
  scripts/
    bootstrap_consumer.sh      # no-infra smoke test of the consumer
    bootstrap_es_lookup.sh     # no-infra smoke test of the lookup
    bootstrap_e2e_docker.sh    # full live end-to-end
  src/
    etl/
      __init__.py
      config.py
      json_io.py
      message.py
      es_lookup.py
      kafka_consumer.py
      kafka_producer.py
  tests/
    test_message.py
    test_es_lookup.py
    test_kafka_consumer.py
  docker-compose.yml
  pyproject.toml
  README.md
```

We will build strictly in dependency order: config → json_io → message →
es_lookup → kafka_consumer → kafka_producer → scripts → docker → end-to-end.

---

## Stage 0 — Prerequisites and the empty project

You need:

- **Python 3.11+** (the project targets 3.11; 3.12 runs it fine — it uses the standard
  library `tomllib`, which landed in 3.11).
- **Docker** with Compose (only needed from Stage 11 onward).

Create the skeleton and a virtual environment:

```bash
mkdir kafka-codex && cd kafka-codex
mkdir -p src/etl tests config samples scripts docs data/converted

python3.11 -m venv .venv        # or python3 if 3.11 isn't the default
source .venv/bin/activate
python -m pip install --upgrade pip
```

> **Why `src/`?** Putting the package under `src/` prevents Python from importing it
> implicitly from the working directory. You are forced to install the package (or set
> `PYTHONPATH=src`), which means your tests exercise the package the same way a user
> would. This is the "src layout" and it catches packaging mistakes early.

**Checkpoint 0** — confirm the interpreter:

```bash
python --version
# Python 3.12.8        (3.11.x is equally fine)
```

If you see a version `>= 3.11`, continue.

---

## Stage 1 — The package and its build metadata

### `pyproject.toml`

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

- `requires-python = ">=3.11"` — enforces the `tomllib` floor.
- `dependencies` — pinned to major versions. `elasticsearch<9` matters: the 8.x client
  speaks to an 8.x cluster, and the API differs in 9.x. `kafka-python` is the pure-Python
  Kafka client (no C/librdkafka build step — part of why this project is "simple").
- `[project.optional-dependencies] dev` — `pytest` is a dev-only extra, so production
  installs stay lean.
- `[tool.pytest.ini_options]` — this is the quiet hero of the whole tutorial:
  - `pythonpath = ["src"]` tells pytest to add `src/` to `sys.path`, so `import etl`
    works in tests **without** installing the package.
  - `testpaths = ["tests"]` means a bare `pytest` only collects our tests.

### `src/etl/__init__.py`

```python
"""Kafka and Elasticsearch ETL components."""

__all__ = ["config", "kafka_consumer", "es_lookup"]
```

`__all__` documents the public submodules. It does not import them (so importing `etl`
stays cheap and doesn't drag in `kafka`/`elasticsearch`).

Now install the package in editable mode:

```bash
pip install -e .
```

**Checkpoint 1** — the package imports:

```bash
python -c "import etl; print('etl package OK ->', etl.__all__)"
```

Expected:

```text
etl package OK -> ['config', 'kafka_consumer', 'es_lookup']
```

---

## Stage 2 — Environment config files

Three TOML files, one per environment. Start with the one we will actually run against
Docker.

### `config/local.toml`

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
max_poll_interval_ms = 900000

[consumer]
output_dir = "data/converted"
trigger_next = true
next_command = ["python", "-m", "etl.es_lookup"]
batch_max_records = 100
batch_interval_seconds = 600
poll_timeout_ms = 1000

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

Why these specific values:

- `auto_offset_reset = "earliest"` — for `local` we want a brand-new consumer group to
  read messages that were produced *before* it joined. (Dev/prod use `"latest"`; you
  rarely want to reprocess history in those.)
- `[consumer]` knobs:
  - `trigger_next = true` + `next_command` — after saving a converted message, the
    consumer shells out to `etl.es_lookup`. The command is data, not hardcoded, so you
    can repoint it.
  - `batch_interval_seconds = 600` — the live consumer processes at most one batch every
    10 minutes by default (this is a low-volume control topic, not a firehose).
  - `poll_timeout_ms = 1000` — how long a single `poll()` blocks waiting for records.
- `[message] id_attribute = "event_id"` — the field the pipeline treats as the primary
  key. Change this one line to adapt to a different message schema.
- `verify_certs = false` for local because our Docker ES runs plain HTTP with security
  disabled.

### `config/dev.toml`

Same shape, dev-ish values:

```toml
[pipeline]
environment = "dev"

[kafka]
bootstrap_servers = ["dev-kafka-1:9092", "dev-kafka-2:9092"]
topic = "control-topic"
group_id = "etl-dev"
client_id = "etl-control-consumer-dev"
auto_offset_reset = "latest"
enable_auto_commit = true
security_protocol = "PLAINTEXT"
max_poll_interval_ms = 900000

[consumer]
output_dir = "data/converted"
trigger_next = true
next_command = ["python", "-m", "etl.es_lookup"]
batch_max_records = 100
batch_interval_seconds = 600
poll_timeout_ms = 1000

[message]
id_attribute = "event_id"

[elasticsearch]
hosts = ["http://dev-es:9200"]
index = "source-index-dev"
request_timeout_seconds = 30
verify_certs = true
username = ""
password = ""
api_key = ""
```

### `config/prod.toml`

Note the hardened transport — `SASL_SSL` for Kafka, `https` + `verify_certs = true` for ES:

```toml
[pipeline]
environment = "prod"

[kafka]
bootstrap_servers = ["prod-kafka-1:9092", "prod-kafka-2:9092", "prod-kafka-3:9092"]
topic = "control-topic"
group_id = "etl-prod"
client_id = "etl-control-consumer-prod"
auto_offset_reset = "latest"
enable_auto_commit = true
security_protocol = "SASL_SSL"
max_poll_interval_ms = 900000

[consumer]
output_dir = "data/converted"
trigger_next = true
next_command = ["python", "-m", "etl.es_lookup"]
batch_max_records = 100
batch_interval_seconds = 600
poll_timeout_ms = 1000

[message]
id_attribute = "event_id"

[elasticsearch]
hosts = ["https://prod-es:9200"]
index = "source-index-prod"
request_timeout_seconds = 30
verify_certs = true
username = ""
password = ""
api_key = ""
```

There is no test yet — we have no loader. We test these in Stage 3.

---

## Stage 3 — The config loader

### `src/etl/config.py`

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

Walkthrough:

- `from __future__ import annotations` — makes all annotations lazy strings, so syntax
  like `dict[str, Any]` and `Path | None` works cleanly and never costs anything at runtime.
- `PROJECT_ROOT = Path(__file__).resolve().parents[2]` — `__file__` is
  `.../src/etl/config.py`; `.parents[0]` is `etl/`, `[1]` is `src/`, `[2]` is the project
  root. Everything else resolves relative to this anchor, so the tools work no matter your
  current working directory.
- `@dataclass(frozen=True) AppConfig` — a typed, **immutable** snapshot of config.
  Frozen means a stray `config.kafka = ...` raises instead of silently corrupting state.
- `load_config`:
  - `base_dir = config_dir or CONFIG_DIR` — the injectable `config_dir` is a seam for
    tests: point it at a temp dir to test loading without touching real config.
  - The existence check builds a **helpful** error: it lists the envs that *do* exist by
    globbing `*.toml`. A typo'd `--env stagng` tells you exactly what's available.
  - `tomllib.load(handle)` — note the file is opened in **binary** (`"rb"`); `tomllib`
    requires bytes.
  - The `AppConfig(...)` construction uses `raw.get("kafka", {})` everywhere, so a missing
    section degrades to an empty dict instead of a `KeyError`. `env` prefers the value
    inside `[pipeline]`, falling back to the `--env` argument.
- `resolve_project_path` — turns a config value like `"data/converted"` into an absolute
  path under the project root, while leaving already-absolute paths untouched.

**Checkpoint 3** — load the local config live:

```bash
python -c "from etl.config import load_config, resolve_project_path; \
c=load_config('local'); \
print('env:', c.env); print('topic:', c.kafka['topic']); \
print('index:', c.elasticsearch['index']); print('id_attr:', c.message['id_attribute']); \
print('resolved:', resolve_project_path('data/converted'))"
```

Expected:

```text
env: local
topic: control-topic
index: source-index
id_attr: event_id
resolved: /path/to/kafka-codex/data/converted
```

Also confirm the friendly error path:

```bash
python -c "from etl.config import load_config; load_config('nope')" 2>&1 | tail -1
# FileNotFoundError: Config file not found for env 'nope': .../config/nope.toml. Available envs: dev, local, prod
```

---

## Stage 4 — JSON I/O helpers

### `src/etl/json_io.py`

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

Three tiny functions, each with one job:

- `load_json_file` / `parse_json_object` both **validate that the top-level value is a
  dict**. The whole pipeline assumes "a message is a JSON object"; these guards turn a
  silently-wrong input (a JSON array, a bare string) into a clear `ValueError` at the
  boundary instead of an `AttributeError` deep inside business logic.
- `write_json_file`:
  - `path.parent.mkdir(parents=True, exist_ok=True)` — auto-creates `data/converted/`
    on first write; idempotent.
  - `indent=2, sort_keys=True` — deterministic, diff-friendly, human-readable output.
    `sort_keys` in particular makes file contents stable regardless of dict insertion
    order, which keeps the output testable.
  - The trailing `handle.write("\n")` gives a POSIX-friendly final newline.

**Checkpoint 4** — round-trip through disk:

```bash
python -c "
from pathlib import Path; import tempfile
from etl.json_io import write_json_file, load_json_file, parse_json_object
d = Path(tempfile.mkdtemp()) / 'x.json'
write_json_file(d, {'b': 2, 'a': 1})
print('reloaded:', load_json_file(d))
print('on-disk:', repr(d.read_text()))
print('parsed:', parse_json_object('{\"k\":1}'))
"
```

Expected (note keys sorted on disk, trailing newline):

```text
reloaded: {'a': 1, 'b': 2}
on-disk: '{\n  "a": 1,\n  "b": 2\n}\n'
parsed: {'k': 1}
```

---

## Stage 5 — Message conversion (first real unit test)

This is the heart of the consumer: take a raw control message and produce a canonical
envelope. It is pure, so we cover it with a real pytest test.

### `src/etl/message.py`

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

Walkthrough:

- `utc_now_iso()` — produces `2026-06-22T02:10:23.014528Z`. It builds a timezone-aware UTC
  timestamp, then swaps the `+00:00` offset for the `Z` suffix that downstream consumers
  and Elasticsearch date parsers expect.
- `convert_control_message`:
  - The `*` forces `id_attribute`, `source`, `kafka_metadata` to be **keyword-only**.
    Callers must write `convert_control_message(payload, id_attribute=..., source=...)`,
    which keeps call sites self-documenting and prevents positional mix-ups.
  - The guard raises a clear `KeyError` if the configured id field is missing — fail fast,
    at the point of conversion, with the field name in the message.
  - The returned envelope is the canonical shape every later stage relies on:
    - `message_id` — a fresh UUID per conversion (de-dupes output filenames).
    - `converted_at` — when we processed it.
    - `source` — provenance: `"kafka"`, `"manual-file"`, etc.
    - `id_attribute` / `id_value` — the promoted key name and value, so downstream code
      doesn't need to re-read config to find the id.
    - `kafka` — broker metadata (topic/partition/offset/...) or `{}` for non-Kafka sources.
    - `payload` — the original message, untouched.

### `tests/test_message.py`

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

The test asserts the two behaviors that matter: the configured id is **promoted** to
`id_value`, and the original payload is **preserved** verbatim.

**Checkpoint 5** — run just this test (recall `pyproject.toml` already put `src/` on the path):

```bash
python -m pytest tests/test_message.py -q
```

Expected:

```text
.                                                                        [100%]
1 passed in 0.00s
```

---

## Stage 6 — The Elasticsearch lookup component

Now we build the second component end-to-end, but we keep the network behind a
`--dry-run` so we can test it with no cluster running. Pure helpers first, then the
client, then the CLI.

### `src/etl/es_lookup.py`

```python
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from .config import load_config
from .json_io import load_json_file, parse_json_object


def build_query_from_id(id_attribute: str, id_value: str) -> dict[str, Any]:
    return {"query": {"term": {id_attribute: id_value}}}


def normalize_query(query_json: dict[str, Any]) -> dict[str, Any]:
    if "query" in query_json:
        return query_json
    return {"query": query_json}


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

The three pure helpers — this is the testable core:

- `build_query_from_id` — wraps an id into an Elasticsearch `term` query. `term` (not
  `match`) means an exact, non-analyzed match, which is right for keyword ids.
- `normalize_query` — accepts either a full query body (`{"query": {...}}`) or just the
  inner clause (`{"term": {...}}`) and always returns a full body. This lets the
  `--query-json` CLI flag be forgiving about what the user pastes.
- `extract_id_from_message` — finds the id across the three shapes the pipeline can hand
  it, in priority order:
  1. a converted envelope (has `id_value`) — the common case from the consumer;
  2. a converted envelope's nested `payload`;
  3. a raw message where the id sits at the top level.
  If none match, a clear `KeyError`.

Next, the lazy client factory:

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

- The `import elasticsearch` is **inside** the function. If the lib is missing, you get an
  actionable `RuntimeError` — but only if you actually try a live query. Dry runs never hit
  this line.
- Auth is layered: prefer `api_key`; otherwise use `username`/`password` if both are set;
  otherwise no auth (our local plaintext ES). Empty strings in the TOML are falsy, so the
  local config naturally lands in the no-auth branch.

The orchestration + CLI:

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

- `run_lookup` is the seam between CLI and logic. It resolves config, then chooses **one**
  of four input modes (precedence: explicit `--query-json` wins; otherwise
  `--id` > `--message-file` > `--json`; otherwise a clear error).
- `if dry_run:` returns the resolved index + query **before** any client is created — this
  is what makes the component testable and demoable without ES.
- The live branch runs `client.search(...)` and unwraps `response.body` (the 8.x client
  returns an `ObjectApiResponse`; `.body` is the plain dict, which is JSON-serializable).
- `main()` just wires argparse to `run_lookup` and pretty-prints the result.

### `tests/test_es_lookup.py`

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

These three tests pin the exact contracts of the pure helpers — no ES needed.

**Checkpoint 6a** — unit tests:

```bash
python -m pytest tests/test_es_lookup.py -q
```

```text
...                                                                      [100%]
3 passed in 0.00s
```

**Checkpoint 6b** — run the CLI in dry-run (still no cluster):

```bash
python -m etl.es_lookup --env local --id customer-123 --dry-run
```

Expected:

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

We will hit a *live* cluster in Stage 12.

---

## Stage 7 — The Kafka consumer

The biggest module. We build it as a stack of pure helpers (testable), then a file-debug
path (testable without Kafka), then the live poll loop. We also need the sample message
the debug path reads.

### `samples/control_message.json`

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

### `src/etl/kafka_consumer.py` — part 1: imports, defaults, decode

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


DEFAULT_BATCH_INTERVAL_SECONDS = 600.0
DEFAULT_BATCH_MAX_RECORDS = 100
DEFAULT_POLL_TIMEOUT_MS = 1000


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

- The `DEFAULT_*` module constants are the hardcoded fallbacks used when neither the CLI
  nor the config supplies a value.
- `decode_kafka_value` is defensive about its input type: a dict passes through
  (useful in tests), bytes get UTF-8 decoded, strings get JSON-parsed — and the result
  must be an object. It is registered as the consumer's `value_deserializer` below.

### part 2: the client factory and metadata helpers

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

- Same lazy-import pattern as ES. The deserializer is wired here, so every record's
  `.value` arrives already decoded into a dict.
- `message_metadata` uses `getattr(..., None)` so it works on a real Kafka record **or**
  any duck-typed stand-in (the tests pass `SimpleNamespace` objects). It captures the
  broker coordinates we stash under `kafka` in the envelope.
- `_decode_key` safely stringifies the (optional, possibly bytes) message key.

### part 3: output path + triggering the next component

```python
def output_path(output_dir: Path, converted_message: dict[str, Any]) -> Path:
    message_id = str(converted_message["message_id"])
    id_value = str(converted_message["id_value"]).replace("/", "_")
    return output_dir / f"{id_value}-{message_id}.json"


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

- `output_path` builds a filename like `customer-123-<uuid>.json`. The `id_value` is part
  of the name for at-a-glance grep-ability; the UUID guarantees uniqueness; `replace("/", "_")`
  keeps ids that contain slashes from creating accidental subdirectories.
- `trigger_next_component` shells out to the next stage as a **separate process**:
  - `if command[0] == "python": command = [sys.executable, ...]` — swap the literal
    `"python"` from config for the *current* interpreter (`sys.executable`), so the child
    runs inside the same venv. This is important and easy to miss.
  - It appends `--env` and `--message-file`, plus `--dry-run` when asked.
  - `check=True` turns a non-zero exit into an exception; `capture_output=True, text=True`
    collect stdout/stderr as strings so the parent can report them.

### part 4: batch helpers (the most-tested logic)

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

- `flatten_polled_records` — `KafkaConsumer.poll()` returns a dict keyed by
  `TopicPartition` whose values are lists of records. We don't care about partitions here,
  so we flatten to a single list (and treat `None`/empty as `[]`).
- `seconds_until_next_batch` — implements the throttle. Given when the batch started and
  the desired interval, it computes the remaining sleep using `time.monotonic()` (a clock
  that never goes backwards). It never returns negative, and short-circuits to `0` if the
  interval is disabled (`<= 0`).
- `resolve_batch_options` — the **precedence resolver**: CLI flag → config value →
  module default, for each of the three knobs, with validation. This is the single place
  that decides "what batch settings are we actually using," and it's pure, so it's easy to
  test exhaustively.

### part 5: process one payload (used by both debug and live paths)

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

This is the per-message workflow, shared by the file-debug path and the live loop:

1. Load config; read the id attribute.
2. Convert the raw payload into the canonical envelope (Stage 5).
3. Resolve the output dir to an absolute path and write the JSON (Stage 4).
4. Decide whether to trigger the next stage. Note the three-state `trigger_next`:
   `None` means "defer to config's `trigger_next`," while `True`/`False` are explicit CLI
   overrides (`--trigger-next` / `--no-trigger-next`).
5. If triggering, shell out and capture `returncode`/`stdout`/`stderr`.
6. Return a structured result the CLI prints.

### part 6: the live poll loop

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

- It is a **generator** (`yield result`), so a caller streams results as they happen and
  can stop early (the test calls `.close()` on it).
- Resolve batch options once, create the consumer, then loop:
  - record `batch_started_at` for the throttle;
  - `poll(...)` and flatten;
  - empty batch → `continue` (poll again);
  - otherwise process each message and `yield`;
  - if `--once`, break after the first non-empty batch;
  - otherwise sleep the remaining time until the interval elapses.
- `finally: consumer.close()` guarantees the consumer (and its group membership) is
  released even if the caller stops iterating or an error bubbles up.

### part 7: argument parsing and entry point

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

- `trigger_override` collapses the two boolean flags into the tri-state used everywhere:
  both set → error; `--trigger-next` → `True`; `--no-trigger-next` → `False`; neither →
  `None` (defer to config).
- `main()` branches once: if `--message-file` is given it runs the **debug path** (no
  Kafka, `source="manual-file"`); otherwise it drives the **live loop** and prints each
  streamed result, flushing so output appears immediately in a long-running process.

### `tests/test_kafka_consumer.py`

```python
from types import SimpleNamespace

from etl import kafka_consumer


def test_flatten_polled_records_returns_messages_in_partition_order():
    first = SimpleNamespace(value={"event_id": "one"})
    second = SimpleNamespace(value={"event_id": "two"})

    assert kafka_consumer.flatten_polled_records({"partition-0": [first], "partition-1": [second]}) == [
        first,
        second,
    ]


def test_resolve_batch_options_defaults_to_ten_minute_interval():
    max_records, interval_seconds, poll_timeout_ms = kafka_consumer.resolve_batch_options(
        {},
        batch_max_records=None,
        batch_interval_seconds=None,
        poll_timeout_ms=None,
    )

    assert max_records == 100
    assert interval_seconds == 600
    assert poll_timeout_ms == 1000


def test_consume_messages_waits_between_non_empty_batches(monkeypatch):
    first = SimpleNamespace(
        value={"event_id": "one"},
        topic="control-topic",
        partition=0,
        offset=0,
        timestamp=1,
        key=b"one",
    )
    second = SimpleNamespace(
        value={"event_id": "two"},
        topic="control-topic",
        partition=0,
        offset=1,
        timestamp=2,
        key=b"two",
    )

    class FakeConsumer:
        def __init__(self):
            self.poll_calls = 0
            self.closed = False

        def poll(self, *, timeout_ms, max_records):
            self.poll_calls += 1
            assert timeout_ms == 25
            assert max_records == 1
            if self.poll_calls == 1:
                return {"partition-0": [first]}
            if self.poll_calls == 2:
                return {"partition-0": [second]}
            raise AssertionError("test should only poll two batches")

        def close(self):
            self.closed = True

    fake_consumer = FakeConsumer()
    sleeps: list[float] = []
    monotonic_values = iter([0.0, 1.0, 600.0])

    monkeypatch.setattr(
        kafka_consumer,
        "load_config",
        lambda env: SimpleNamespace(
            kafka={},
            consumer={"batch_max_records": 1, "batch_interval_seconds": 600, "poll_timeout_ms": 25},
        ),
    )
    monkeypatch.setattr(kafka_consumer, "create_kafka_consumer", lambda kafka_config: fake_consumer)
    monkeypatch.setattr(
        kafka_consumer,
        "process_payload",
        lambda **kwargs: {"payload": kwargs["payload"], "metadata": kwargs["kafka_metadata"]},
    )
    monkeypatch.setattr(kafka_consumer.time, "monotonic", lambda: next(monotonic_values))
    monkeypatch.setattr(kafka_consumer.time, "sleep", sleeps.append)

    results = kafka_consumer.consume_messages(
        env="local",
        once=False,
        trigger_next=None,
        dry_run_next=False,
    )

    assert next(results)["payload"] == {"event_id": "one"}
    assert sleeps == []
    assert next(results)["payload"] == {"event_id": "two"}
    assert sleeps == [599.0]
    results.close()
    assert fake_consumer.closed is True
```

The first two tests are straightforward unit tests of the pure helpers. The third is the
interesting one — it shows how to test a live-loop **without Kafka**:

- It `monkeypatch`es `load_config`, `create_kafka_consumer`, and `process_payload` so no
  real config/broker/conversion runs.
- It injects a `FakeConsumer` whose `poll()` returns two batches then refuses a third — and
  asserts the loop calls `poll` with the *resolved* options (`timeout_ms=25`, `max_records=1`).
- It fakes `time.monotonic()` to yield `0.0, 1.0, 600.0` and captures `time.sleep` calls in
  a list. The key assertion: after the **first** batch there is no sleep yet
  (`sleeps == []`), and after the **second** batch the throttle sleeps `599.0` seconds
  (600 interval − 1 elapsed). This proves `seconds_until_next_batch` is wired into the loop
  correctly.
- Finally it `.close()`s the generator and asserts the consumer was closed — proving the
  `finally` cleanup runs.

**Checkpoint 7a** — the consumer unit tests:

```bash
python -m pytest tests/test_kafka_consumer.py -q
```

```text
...                                                                      [100%]
3 passed in 0.00s
```

**Checkpoint 7b** — the file-debug path, with triggering off (no Kafka, no ES):

```bash
python -m etl.kafka_consumer --env local \
  --message-file samples/control_message.json --no-trigger-next
```

You'll see the full converted envelope; the tail confirms it was saved and nothing was
triggered:

```json
  "next_result": null,
  "saved_file": ".../data/converted/customer-123-<uuid>.json"
}
```

A converted file now exists under `data/converted/`.

---

## Stage 8 — The Kafka producer (for integration testing)

We need a way to put a message on the topic for the end-to-end test.

### `src/etl/kafka_producer.py`

```python
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from .config import load_config
from .json_io import load_json_file, parse_json_object


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

- Same lazy-import discipline. The producer's `key_serializer`/`value_serializer` mirror
  the consumer's deserializer: keys are UTF-8 bytes (or `None`), values are JSON-encoded
  bytes — so the consumer's `decode_kafka_value` round-trips them perfectly.
- `publish_message` sends one message, **blocks** on `future.get(timeout=30)` to surface
  delivery errors, flushes, and returns the broker-assigned `topic`/`partition`/`offset`.
  `finally: producer.close()` always releases the socket.

There is no unit test for the producer — it is pure I/O glue, and it gets fully exercised
by the live end-to-end script in Stage 12. That is a deliberate choice: test pure logic
with fast unit tests, test I/O glue with one real integration run.

---

## Stage 9 — Bootstrap smoke-test scripts (no infrastructure)

These give you one-command confidence in the wiring before any container exists.

### `scripts/bootstrap_consumer.sh`

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

- `set -euo pipefail` — exit on error, error on unset vars, fail a pipeline if any stage
  fails. Standard safe-bash preamble.
- `cd "$(dirname "$0")/.."` — run from the project root regardless of where you invoke it.
- It exercises the **full consumer → trigger → lookup chain**, but with `--dry-run-next`,
  so the triggered `es_lookup` prints its query instead of calling a cluster.

### `scripts/bootstrap_es_lookup.sh`

```bash
#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
PYTHONPATH="${PYTHONPATH:-}:src" python -m etl.es_lookup \
  --env local \
  --json '{"event_id":"customer-123","event_type":"manual.debug"}' \
  --dry-run
```

Exercises the lookup's raw-JSON input mode in dry-run.

Make them executable:

```bash
chmod +x scripts/*.sh
```

> **Heads-up about `python` vs your venv.** These two scripts call `python` literally. If
> your virtualenv's `python` isn't first on `PATH`, you'll get `python: command not found`.
> Either activate the venv (`source .venv/bin/activate`) or prepend it:
> `export PATH="$PWD/.venv/bin:$PATH"`. (The e2e script in Stage 12 sidesteps this by
> honoring a `PYTHON` variable.)

**Checkpoint 9a** — consumer smoke test:

```bash
./scripts/bootstrap_consumer.sh
```

The triggered lookup result is embedded as a string under `next_result.stdout`, and its
`returncode` is `0`:

```json
  "next_result": {
    "returncode": 0,
    "stderr": "",
    "stdout": "{\n  \"dry_run\": true,\n  \"index\": \"source-index\", ... }"
  },
```

**Checkpoint 9b** — lookup smoke test:

```bash
./scripts/bootstrap_es_lookup.sh
```

```json
{
  "dry_run": true,
  "index": "source-index",
  "query": { "query": { "term": { "event_id": "customer-123" } } }
}
```

At this point the entire pipeline is proven **without any infrastructure**. Everything
from here on is live.

---

## Stage 10 — Local Kafka and Elasticsearch with Docker Compose

### `docker-compose.yml`

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

Why these settings:

- **Kafka is in KRaft mode** (no ZooKeeper). A single node plays both `broker` and
  `controller` roles. This is the modern, lightweight way to run Kafka for dev.
- `KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092` — critical: this is the address
  the broker hands back to clients. Because we publish the port to `localhost:9092`, your
  host-side Python connects correctly.
- The three `..._REPLICATION_FACTOR: 1` / `MIN_ISR: 1` settings are required because a
  single broker cannot satisfy the default replication factor of 3.
- `KAFKA_AUTO_CREATE_TOPICS_ENABLE: true` — so `control-topic` is created on first
  produce; we don't need a separate topic-creation step.
- **Elasticsearch**: `single-node` discovery and `xpack.security.enabled: false` give us
  plain HTTP on `:9200` with no auth — which is exactly what `config/local.toml`
  (`http://localhost:9200`, empty credentials, `verify_certs = false`) expects. The
  `ES_JAVA_OPTS` cap keeps the JVM heap modest for a laptop.

Start them:

```bash
docker compose up -d kafka elasticsearch
```

**Checkpoint 10** — both containers up and ES answering:

```bash
docker ps --filter name=kafka-codex --format 'table {{.Names}}\t{{.Status}}'
curl -fsS http://localhost:9200 | head -5
```

Expected (ES may take 10–30s to become reachable on first boot):

```text
NAMES                       STATUS
kafka-codex-kafka           Up ...
kafka-codex-elasticsearch   Up ...
```
```json
{
  "name" : "...",
  "cluster_name" : "docker-cluster",
  ...
```

---

## Stage 11 — A live single-stage check before the full chain

Before the orchestrated end-to-end, prove each component talks to its backend on its own.

**Checkpoint 11a** — index one document, then query it live through `es_lookup`:

```bash
# seed a document the lookup will find
curl -fsS -X PUT "http://localhost:9200/source-index" \
  -H "Content-Type: application/json" \
  -d '{"mappings":{"properties":{"event_id":{"type":"keyword"}}}}' >/dev/null || true
curl -fsS -X POST "http://localhost:9200/source-index/_doc/customer-123?refresh=true" \
  -H "Content-Type: application/json" \
  -d '{"event_id":"customer-123","name":"Demo Customer"}' >/dev/null

# live lookup (note: NO --dry-run)
python -m etl.es_lookup --env local --id customer-123
```

You should see a real hit with `dry_run: false` and `hits.total.value: 1`. The
`event_id` as a `keyword` field is what makes the `term` query match exactly.

**Checkpoint 11b** — publish + consume one live batch (consumer triggers a real lookup):

```bash
python -m etl.kafka_producer --env local --message-file samples/control_message.json --key customer-123
python -m etl.kafka_consumer --env local --once --trigger-next
```

The consumer reads the message (you'll see real `kafka` metadata — topic/partition/offset),
writes the envelope, and the triggered `es_lookup` returns the document. Because
`local.toml` has `auto_offset_reset = "earliest"`, a fresh consumer group still sees the
message you just produced.

> If `--once` seems to hang: the loop keeps polling until it gets a non-empty batch. Make
> sure the producer actually published (it prints an offset) and that you're using the
> `local` env so the group resets to `earliest`.

---

## Stage 12 — The end-to-end script

Now wrap Checkpoints 11a/11b into one script that does the whole flow from scratch.

### `scripts/bootstrap_e2e_docker.sh`

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

What it does, step by step:

1. `PYTHON="${PYTHON:-.venv/bin/python}"` — defaults to the venv interpreter, but you can
   override (`PYTHON=python ./scripts/...`). This is why the e2e script doesn't suffer the
   `python: command not found` gotcha the smoke scripts have.
2. **Wait for ES** — polls `http://localhost:9200` up to 60 times (2s apart = 2 min) so a
   cold container has time to come up.
3. **Create the index** with an explicit mapping (`event_id` as `keyword`). `|| true`
   makes re-runs idempotent — a 400 "already exists" won't abort the script.
4. **Index a document** with `?refresh=true` so it's immediately searchable (no waiting for
   the periodic refresh).
5. **Produce** the sample control message to `control-topic` with a key.
6. **Consume one batch** with `--trigger-next`: convert → save → trigger the live ES lookup.

`chmod +x scripts/bootstrap_e2e_docker.sh`, then:

**Checkpoint 12** — the full live pipeline:

```bash
PYTHON=.venv/bin/python ./scripts/bootstrap_e2e_docker.sh
```

The final JSON block is the proof. The consumer output shows real Kafka metadata, and the
triggered lookup (inside `next_result.stdout`) returns the indexed document:

```json
  "converted": {
    ...
    "kafka": { "key": "customer-123", "offset": 1, "partition": 0, "topic": "control-topic", ... },
    "source": "kafka"
  },
  "next_result": {
    "returncode": 0,
    "stdout": "{ ... \"hits\": { ... \"_source\": { \"event_id\": \"customer-123\", \"name\": \"Demo Customer\", ... }, \"total\": { \"value\": 1 } } ... }"
  },
  "saved_file": ".../data/converted/customer-123-<uuid>.json"
```

`offset` increments each run (the topic retains prior messages); `hits.total.value: 1`
confirms the lookup found the document. **The pipeline works end to end against live Kafka
and Elasticsearch.**

---

## Stage 13 — README and the full regression run

Add a `README.md` describing install, configs, the smoke tests, the Docker flow, and the
CLI usage of each component (see the repo's `README.md` for the canonical text).

Finally, the whole-suite regression — run every unit test at once:

**Checkpoint 13** — full test suite:

```bash
python -m pytest -q
```

Expected:

```text
.......                                                                  [100%]
7 passed in 0.08s
```

Seven tests: 1 message + 3 es_lookup + 3 kafka_consumer. Fast, because they're all pure
logic — the slow, live parts are covered by the bootstrap scripts.

---

## How it all fits together (the mental model)

```
config/*.toml ──load_config──► AppConfig (frozen)
                                   │
samples/*.json ──load_json_file──► payload
                                   │
                        convert_control_message  (message.py)
                                   │  canonical envelope
                          write_json_file (json_io.py) ──► data/converted/<id>-<uuid>.json
                                   │
                  trigger_next_component (subprocess: python -m etl.es_lookup)
                                   │
                  run_lookup: extract id ► build_query ► client.search ► hits
```

- **`config.py`** anchors paths and loads typed, immutable config.
- **`json_io.py`** is the validated read/write boundary.
- **`message.py`** is the pure conversion.
- **`es_lookup.py`** and **`kafka_consumer.py`** each follow the same shape: pure helpers
  (unit-tested) + lazy client factory (no infra until needed) + `run_*`/`consume_*`
  orchestrator + thin argparse `main`.
- **`kafka_producer.py`** exists only to feed the live test.

## The testing strategy, summarized

| Layer | What | How verified |
|---|---|---|
| Pure functions | conversion, query building, id extraction, batch resolution, flatten, throttle | `pytest` (7 tests, no infra) |
| Component wiring | consumer → save → trigger → lookup | `bootstrap_consumer.sh` / `bootstrap_es_lookup.sh` (dry-run, no infra) |
| Live single stage | each component vs its backend | Checkpoints 11a / 11b |
| Live end-to-end | produce → consume → convert → save → query | `bootstrap_e2e_docker.sh` |

This is the discipline to carry forward: **never add a feature without a checkpoint that
proves it.** Pure logic gets a fast unit test; I/O glue gets a live run. If a checkpoint
fails, you fix it before writing the next line.

## Recommended rebuild order (recap)

1. Stage 0 — skeleton + venv
2. Stage 1 — `pyproject.toml`, `__init__.py`, `pip install -e .` → *import works*
3. Stage 2–3 — config files + `config.py` → *load_config works*
4. Stage 4 — `json_io.py` → *round-trip works*
5. Stage 5 — `message.py` + test → *1 test passes*
6. Stage 6 — `es_lookup.py` + tests → *3 tests + dry-run work*
7. Stage 7 — `kafka_consumer.py` + tests → *3 tests + file-debug work*
8. Stage 8 — `kafka_producer.py`
9. Stage 9 — bootstrap scripts → *no-infra chain works*
10. Stage 10 — `docker-compose.yml` → *containers up*
11. Stage 11 — live single-stage checks
12. Stage 12 — `bootstrap_e2e_docker.sh` → *live end-to-end works*
13. Stage 13 — README + `pytest -q` → *7 passed*

When you can run `pytest -q` (7 passed) and `bootstrap_e2e_docker.sh` (a live hit) on a
clean checkout, you have faithfully rebuilt the project.

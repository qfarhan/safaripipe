from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Iterable, Iterator

from .config import load_config, resolve_project_path
from .json_io import get_nested, load_json_file, parse_json_object


DEFAULT_SCROLL_SIZE = 1000
DEFAULT_SCROLL_TIMEOUT = "2m"
DEFAULT_OUTPUT_DIR = "data/es_results"


def build_query_from_id(id_attribute: str, id_value: str) -> dict[str, Any]:
    return {"query": {"term": {id_attribute: id_value}}}


def normalize_query(query_json: dict[str, Any]) -> dict[str, Any]:
    if "query" in query_json:
        return query_json
    return {"query": query_json}


def extract_id_from_message(message: dict[str, Any], id_attribute: str) -> str:
    # Converted messages carry the resolved value flat in id_value, so a nested
    # id_attribute does not need re-walking here.
    if "id_value" in message:
        return str(message["id_value"])

    # Raw messages: id_attribute may be a dotted path (e.g. "header.batchId"),
    # found either inside the payload wrapper or at the top level.
    payload = message.get("payload")
    if isinstance(payload, dict):
        try:
            return str(get_nested(payload, id_attribute))
        except KeyError:
            pass

    try:
        return str(get_nested(message, id_attribute))
    except KeyError as exc:
        raise KeyError(
            f"Could not find ID attribute '{id_attribute}' in message"
        ) from exc


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


def scan_all_hits(
    client: Any, *, index: str, query: dict[str, Any], size: int, scroll_timeout: str
) -> Iterator[dict[str, Any]]:
    """Yield every hit matching query, paging past the default 10-doc / 10k window cap.

    A plain client.search only returns the first page (size defaults to 10, and
    even a larger explicit size is capped at index.max_result_window, 10000 by
    default). A batch's record count can exceed both, so this uses the scroll
    API (via the elasticsearch-py scan helper) to keep paging until every
    matching document has been read.
    """
    from elasticsearch.helpers import scan

    return scan(client, index=index, query=query, size=size, scroll=scroll_timeout)


def output_results_path(output_dir: Path, index: str, id_value: str) -> Path:
    safe_id = str(id_value).replace("/", "_")
    return output_dir / f"{index}-{safe_id}.jsonl"


def write_hits_jsonl(path: Path, hits: Iterable[dict[str, Any]]) -> int:
    """Stream hits to a JSON-lines file (one doc per line) instead of holding
    the whole result set in memory or printing thousands of records to the
    terminal."""
    path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with path.open("w", encoding="utf-8") as handle:
        for hit in hits:
            handle.write(json.dumps(hit, sort_keys=True))
            handle.write("\n")
            count += 1
    return count


def run_lookup(
    *,
    env: str,
    message_file: str | None = None,
    raw_json: str | None = None,
    query_json: str | None = None,
    direct_id: str | None = None,
    dry_run: bool = False,
    output_file: str | None = None,
) -> dict[str, Any]:
    config = load_config(env)
    id_attribute = str(config.message.get("id_attribute", "event_id"))
    # The field the value is matched against in Elasticsearch can differ from the
    # JSON path it was read from: when batchId is mapped as `text` with a keyword
    # subfield, a term query must target `header.batchId.keyword`. term_field
    # overrides the query field; it defaults to id_attribute when unset.
    term_field = str(config.elasticsearch.get("term_field", "") or id_attribute)
    index = str(config.elasticsearch.get("index", "source-index"))

    id_value_for_filename = "query"
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
        query = build_query_from_id(term_field, id_value)
        id_value_for_filename = id_value

    if dry_run:
        return {"dry_run": True, "index": index, "query": query}

    client = create_es_client(config.elasticsearch)

    # A batch can span thousands of records (well past the default 10-hit page
    # and the 10k max_result_window), so every matching doc is scrolled and
    # streamed straight to disk rather than collected into one big response or
    # printed to the terminal.
    scroll_size = int(config.elasticsearch.get("scroll_size", DEFAULT_SCROLL_SIZE))
    scroll_timeout = str(config.elasticsearch.get("scroll_timeout", DEFAULT_SCROLL_TIMEOUT))
    hits = scan_all_hits(client, index=index, query=query, size=scroll_size, scroll_timeout=scroll_timeout)

    destination = (
        Path(output_file)
        if output_file
        else output_results_path(
            resolve_project_path(config.elasticsearch.get("output_dir", DEFAULT_OUTPUT_DIR)),
            index,
            str(id_value_for_filename),
        )
    )
    record_count = write_hits_jsonl(destination, hits)

    return {
        "dry_run": False,
        "index": index,
        "query": query,
        "output_file": str(destination),
        "record_count": record_count,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Query Elasticsearch from a Kafka control JSON message.")
    parser.add_argument("--env", default="local", help="Config environment name, e.g. local/dev/prod.")
    parser.add_argument("--message-file", help="Converted Kafka JSON file to read.")
    parser.add_argument("--json", dest="raw_json", help="Raw JSON object containing the configured ID attribute.")
    parser.add_argument("--query-json", help="Elasticsearch query JSON. May be a query body or just the query clause.")
    parser.add_argument("--id", dest="direct_id", help="Direct ID value to search for.")
    parser.add_argument("--dry-run", action="store_true", help="Print the query without calling Elasticsearch.")
    parser.add_argument(
        "--output-file",
        help="Where to write matching records as JSON-lines. Defaults to "
        "'<elasticsearch.output_dir>/<index>-<id_value>.jsonl'.",
    )
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
        output_file=args.output_file,
    )
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

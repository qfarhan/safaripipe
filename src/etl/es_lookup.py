from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from .config import load_config
from .json_io import get_nested, load_json_file, parse_json_object


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

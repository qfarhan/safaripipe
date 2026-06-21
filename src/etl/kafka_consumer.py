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

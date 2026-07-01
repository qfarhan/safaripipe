from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Iterable

from .avro_io import create_avro_deserializer, create_schema_registry_client
from .config import consumer_client_config, load_config, resolve_project_path
from .json_io import load_json_file, write_json_file
from .message import convert_control_message, status_matches


DEFAULT_POLL_TIMEOUT_SECONDS = 10.0
DEFAULT_MAX_MESSAGES = 100


def create_kafka_consumer(client_config: dict[str, Any]):
    try:
        from confluent_kafka import Consumer
    except ImportError as exc:
        raise RuntimeError(
            "The 'confluent-kafka' package is required for live Kafka consumption. "
            "Install dependencies with 'pip install -e .' or use --message-file for debug runs."
        ) from exc

    return Consumer(dict(client_config))


def message_metadata(message: Any) -> dict[str, Any]:
    return {
        "topic": message.topic(),
        "partition": message.partition(),
        "offset": message.offset(),
        "timestamp": _timestamp_ms(message),
        "key": _decode_key(message.key()),
    }


def _timestamp_ms(message: Any) -> int | None:
    # confluent Message.timestamp() -> (timestamp_type, value); type 0 == NOT_AVAILABLE.
    ts_type, ts_value = message.timestamp()
    return None if ts_type == 0 else ts_value


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


def resolve_topics(kafka_config: dict[str, Any]) -> list[str]:
    """Topics to subscribe to: prefer a 'topics' list, else the single 'topic'.

    One consumer (and one consumer group) can read many topics at once, so the
    EOD batch can drain several sources in a single run. Each message still
    carries its own topic in message_metadata, so downstream routing (e.g. per
    topic Elasticsearch index) can tell them apart.
    """
    topics = kafka_config.get("topics")
    if topics:
        return [str(topic) for topic in topics]
    return [str(kafka_config["topic"])]


def resolve_consume_options(
    consumer_config: dict[str, Any],
    *,
    max_messages: int | None,
    poll_timeout_seconds: float | None,
) -> tuple[int, float]:
    resolved_max = int(
        max_messages
        if max_messages is not None
        else consumer_config.get("max_messages", DEFAULT_MAX_MESSAGES)
    )
    resolved_timeout = float(
        poll_timeout_seconds
        if poll_timeout_seconds is not None
        else consumer_config.get("poll_timeout_seconds", DEFAULT_POLL_TIMEOUT_SECONDS)
    )
    if resolved_max < 0:
        raise ValueError("max_messages cannot be negative (use 0 for unlimited)")
    if resolved_timeout <= 0:
        raise ValueError("poll_timeout_seconds must be greater than 0")
    return resolved_max, resolved_timeout


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

    # Save-gate: only messages whose status field matches the configured value
    # (e.g. control.batch.processStatus == "End") are converted and saved. A
    # message that does not clear the gate is acknowledged but skipped — nothing
    # is written and the next component is not triggered.
    status_field = str(config.message.get("status_field", ""))
    status_value = config.message.get("status_value", "")
    if not status_matches(payload, status_field, status_value):
        return {
            "saved_file": None,
            "skipped": True,
            "skip_reason": f"{status_field} != {status_value!r}",
            "converted": None,
            "next_result": None,
        }

    id_attribute = str(config.message.get("id_attribute", "event_id"))
    converted = convert_control_message(
        payload,
        id_attribute=id_attribute,
        source=source,
        kafka_metadata=kafka_metadata,
    )

    destination = output_path(
        resolve_project_path(config.consumer.get("output_dir", "data/converted")), converted
    )
    write_json_file(destination, converted)

    should_trigger = (
        bool(config.consumer.get("trigger_next", False)) if trigger_next is None else trigger_next
    )
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

    return {
        "saved_file": str(destination),
        "skipped": False,
        "converted": converted,
        "next_result": next_result,
    }


def consume_messages(
    *,
    env: str,
    once: bool,
    trigger_next: bool | None,
    dry_run_next: bool,
    max_messages: int | None = None,
    poll_timeout_seconds: float | None = None,
) -> Iterable[dict[str, Any]]:
    from confluent_kafka import KafkaError, KafkaException
    from confluent_kafka.serialization import MessageField, SerializationContext

    config = load_config(env)
    resolved_max, resolved_timeout = resolve_consume_options(
        config.consumer,
        max_messages=max_messages,
        poll_timeout_seconds=poll_timeout_seconds,
    )

    consumer = create_kafka_consumer(consumer_client_config(config))
    deserializer = create_avro_deserializer(create_schema_registry_client(config.schema_registry))
    topics = resolve_topics(config.kafka)
    consumer.subscribe(topics)

    consumed = 0
    try:
        while True:
            message = consumer.poll(timeout=resolved_timeout)
            if message is None:
                if once:
                    break
                continue
            if message.error():
                # End-of-partition is informational, not a failure.
                if message.error().code() == KafkaError._PARTITION_EOF:
                    if once:
                        break
                    continue
                raise KafkaException(message.error())

            payload = deserializer(
                message.value(), SerializationContext(message.topic(), MessageField.VALUE)
            )
            if payload is None:  # tombstone / null value
                continue

            yield process_payload(
                payload=payload,
                env=env,
                source="kafka",
                kafka_metadata=message_metadata(message),
                trigger_next=trigger_next,
                dry_run_next=dry_run_next,
            )

            consumed += 1
            if resolved_max and consumed >= resolved_max:
                break
    finally:
        consumer.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Consume Kafka Avro control messages and save converted JSON."
    )
    parser.add_argument("--env", default="local", help="Config environment name, e.g. local/dev/prod.")
    parser.add_argument(
        "--once", action="store_true", help="Stop once the topic is drained (poll timeout / EOF)."
    )
    parser.add_argument(
        "--message-file",
        help="Debug mode: read this JSON file instead of Kafka (no Avro/Schema Registry).",
    )
    parser.add_argument("--trigger-next", action="store_true", help="Trigger the Elasticsearch lookup component.")
    parser.add_argument("--no-trigger-next", action="store_true", help="Do not trigger the next component.")
    parser.add_argument("--dry-run-next", action="store_true", help="Pass --dry-run to the next component.")
    parser.add_argument("--max-messages", type=int, help="Stop after N messages (0 = unlimited).")
    parser.add_argument(
        "--poll-timeout-seconds", type=float, help="Seconds each poll() waits for a record. Default 10."
    )
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
        max_messages=args.max_messages,
        poll_timeout_seconds=args.poll_timeout_seconds,
    ):
        print(json.dumps(result, indent=2, sort_keys=True))
        sys.stdout.flush()


if __name__ == "__main__":
    main()

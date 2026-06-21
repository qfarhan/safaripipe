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

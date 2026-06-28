from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from .avro_io import create_avro_serializer, create_schema_registry_client, load_schema_str
from .config import load_config, producer_client_config, resolve_project_path
from .json_io import load_json_file, parse_json_object


DEFAULT_VALUE_SCHEMA_FILE = "samples/control_message.avsc"


def create_kafka_producer(client_config: dict[str, Any]):
    try:
        from confluent_kafka import Producer
    except ImportError as exc:
        raise RuntimeError(
            "The 'confluent-kafka' package is required to publish Kafka messages. "
            "Install dependencies with 'pip install -e .'."
        ) from exc

    return Producer(dict(client_config))


def publish_message(
    *,
    env: str,
    payload: dict[str, Any],
    key: str | None = None,
    topic: str | None = None,
    schema_file: str | None = None,
) -> dict[str, Any]:
    from confluent_kafka.serialization import MessageField, SerializationContext

    config = load_config(env)
    selected_topic = topic or str(config.kafka["topic"])

    schema_path = resolve_project_path(
        schema_file or config.kafka.get("value_schema_file", DEFAULT_VALUE_SCHEMA_FILE)
    )
    sr_client = create_schema_registry_client(config.schema_registry)
    serializer = create_avro_serializer(sr_client, load_schema_str(schema_path))
    value_bytes = serializer(payload, SerializationContext(selected_topic, MessageField.VALUE))

    producer = create_kafka_producer(producer_client_config(config))
    delivery: dict[str, Any] = {}

    def on_delivery(err: Any, message: Any) -> None:
        if err is not None:
            delivery["error"] = err
            return
        delivery["topic"] = message.topic()
        delivery["partition"] = message.partition()
        delivery["offset"] = message.offset()

    producer.produce(
        selected_topic,
        key=key.encode("utf-8") if key is not None else None,
        value=value_bytes,
        on_delivery=on_delivery,
    )
    producer.flush(30)

    if delivery.get("error") is not None:
        from confluent_kafka import KafkaException

        raise KafkaException(delivery["error"])

    return {
        "topic": delivery.get("topic", selected_topic),
        "partition": delivery.get("partition"),
        "offset": delivery.get("offset"),
        "key": key,
        "payload": payload,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Publish an Avro control message to Kafka.")
    parser.add_argument("--env", default="local", help="Config environment name, e.g. local/dev/prod.")
    parser.add_argument("--message-file", help="JSON message file to publish (encoded as Avro).")
    parser.add_argument("--json", dest="raw_json", help="Raw JSON object to publish (encoded as Avro).")
    parser.add_argument("--topic", help="Override Kafka topic from config.")
    parser.add_argument("--key", help="Optional Kafka message key.")
    parser.add_argument("--schema-file", help="Override the Avro value schema (.avsc) path.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.message_file:
        payload = load_json_file(Path(args.message_file))
    elif args.raw_json:
        payload = parse_json_object(args.raw_json)
    else:
        raise ValueError("Provide --message-file or --json")

    result = publish_message(
        env=args.env,
        payload=payload,
        key=args.key,
        topic=args.topic,
        schema_file=args.schema_file,
    )
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

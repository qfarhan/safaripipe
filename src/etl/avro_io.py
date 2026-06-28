from __future__ import annotations

from pathlib import Path
from typing import Any


def load_schema_str(path: Path) -> str:
    """Read an Avro schema (.avsc) file into a string for the (de)serializer."""
    return path.read_text(encoding="utf-8")


def create_schema_registry_client(sr_config: dict[str, Any]):
    """Create a confluent SchemaRegistryClient from native SR properties.

    Imported lazily so non-Avro / dry-run paths never require the library.
    """
    try:
        from confluent_kafka.schema_registry import SchemaRegistryClient
    except ImportError as exc:  # pragma: no cover - import guard
        raise RuntimeError(
            "The 'confluent-kafka[avro]' package is required for Schema Registry. "
            "Install dependencies with 'pip install -e .'."
        ) from exc

    if not sr_config.get("url"):
        raise ValueError("Schema Registry config is missing a 'url' (set [schema_registry].url)")
    return SchemaRegistryClient(dict(sr_config))


def create_avro_deserializer(sr_client, schema_str: str | None = None):
    """Avro deserializer (bytes -> dict).

    With schema_str=None it auto-detects the writer schema from the 4-byte schema
    ID embedded in each message (the recommended consumer mode).
    """
    from confluent_kafka.schema_registry.avro import AvroDeserializer

    if schema_str is None:
        return AvroDeserializer(sr_client)
    return AvroDeserializer(sr_client, schema_str)


def create_avro_serializer(sr_client, schema_str: str):
    """Avro serializer (dict -> bytes), registering the schema under <topic>-value."""
    from confluent_kafka.schema_registry.avro import AvroSerializer

    return AvroSerializer(sr_client, schema_str)

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
    # kafka holds: "topic", optional "value_schema_file", and the librdkafka
    # property tables "client" (shared), "consumer", and "producer".
    kafka: dict[str, Any]
    # schema_registry holds confluent SchemaRegistryClient properties, e.g. "url".
    schema_registry: dict[str, Any]
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
        schema_registry=dict(raw.get("schema_registry", {})),
        consumer=dict(raw.get("consumer", {})),
        message=dict(raw.get("message", {})),
        elasticsearch=dict(raw.get("elasticsearch", {})),
    )


def resolve_project_path(path_value: str | Path) -> Path:
    path = Path(path_value)
    if path.is_absolute():
        return path
    return PROJECT_ROOT / path


def consumer_client_config(config: AppConfig) -> dict[str, Any]:
    """Build the librdkafka config for a Consumer.

    The shared connection/security props in [kafka.client] are merged with the
    consumer-only props in [kafka.consumer] (group.id, auto.offset.reset, ...).
    Producer-only props are never included, because librdkafka rejects unknown
    or wrong-role properties.
    """
    merged = dict(config.kafka.get("client", {}))
    merged.update(config.kafka.get("consumer", {}))
    return merged


def producer_client_config(config: AppConfig) -> dict[str, Any]:
    """Build the librdkafka config for a Producer ([kafka.client] + [kafka.producer])."""
    merged = dict(config.kafka.get("client", {}))
    merged.update(config.kafka.get("producer", {}))
    return merged

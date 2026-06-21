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

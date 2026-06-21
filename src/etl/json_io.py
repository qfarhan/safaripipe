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

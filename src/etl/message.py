from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import uuid4


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def convert_control_message(
    payload: dict[str, Any],
    *,
    id_attribute: str,
    source: str,
    kafka_metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if id_attribute not in payload:
        raise KeyError(f"Control message is missing required ID attribute '{id_attribute}'")

    return {
        "message_id": str(uuid4()),
        "converted_at": utc_now_iso(),
        "source": source,
        "id_attribute": id_attribute,
        "id_value": payload[id_attribute],
        "kafka": kafka_metadata or {},
        "payload": payload,
    }

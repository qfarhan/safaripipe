from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from .json_io import get_nested


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def status_matches(payload: dict[str, Any], status_field: str, status_value: Any) -> bool:
    """Decide whether a control message clears the save-gate.

    The consumer only converts and saves a message when its status field equals
    the configured value (e.g. control.batch.processStatus == "End"). status_field
    is a dotted path (see json_io.get_nested), so it can point at a nested field.

    - An empty/unset status_field disables the gate (everything passes), keeping
      behaviour backwards compatible when no gate is configured.
    - A missing field counts as "no match" so partial/intermediate messages are
      skipped rather than crashing the consumer.
    """
    if not status_field:
        return True
    try:
        actual = get_nested(payload, status_field)
    except KeyError:
        return False
    return str(actual) == str(status_value)


def convert_control_message(
    payload: dict[str, Any],
    *,
    id_attribute: str,
    source: str,
    kafka_metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    # id_attribute may be a dotted path into a nested object, e.g. "header.batchId".
    try:
        id_value = get_nested(payload, id_attribute)
    except KeyError as exc:
        raise KeyError(
            f"Control message is missing required ID attribute '{id_attribute}'"
        ) from exc

    return {
        "message_id": str(uuid4()),
        "converted_at": utc_now_iso(),
        "source": source,
        "id_attribute": id_attribute,
        "id_value": id_value,
        "kafka": kafka_metadata or {},
        "payload": payload,
    }

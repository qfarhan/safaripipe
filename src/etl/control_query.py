from __future__ import annotations

from typing import Any

from .es_lookup import scan_all_hits
from .json_io import get_nested


DEFAULT_SCROLL_SIZE = 1000
DEFAULT_SCROLL_TIMEOUT = "2m"


def build_control_query(
    *, action_field: str, action_value: str, date_field: str, date_value: str
) -> dict[str, Any]:
    """Exact, unscored query for a feed's completed batches on one business date.

    Both clauses are term queries in FILTER context: the control index stores
    action/date as keyword/date fields, so `match` (analyzed, scored) would be
    wrong — a term filter is exact and lets ES skip scoring entirely.
    """
    return {
        "query": {
            "bool": {
                "filter": [
                    {"term": {action_field: action_value}},
                    {"term": {date_field: date_value}},
                ]
            }
        }
    }


def find_completed_batches(
    client: Any,
    *,
    control_index: str,
    action_field: str,
    action_value: str,
    date_field: str,
    date_value: str,
    batch_id_field: str,
    batch_size_field: str | None = None,
    scroll_size: int = DEFAULT_SCROLL_SIZE,
    scroll_timeout: str = DEFAULT_SCROLL_TIMEOUT,
) -> list[dict[str, Any]]:
    """Return one entry per completed batchId found in the control index.

    control_index should be the ALIAS (e.g. ...eod.control.v1), never the
    versioned -r1 index behind it. Every hit is paged (scroll), because a busy
    day can produce more control docs than the default 10-hit page.

    Duplicate control docs carrying the same batchId (e.g. a re-emitted End)
    collapse to one entry — the last doc wins, so a corrected batchSizeIntended
    is the one asserted against.
    """
    query = build_control_query(
        action_field=action_field,
        action_value=action_value,
        date_field=date_field,
        date_value=date_value,
    )

    batches: dict[str, dict[str, Any]] = {}
    hits = scan_all_hits(
        client, index=control_index, query=query, size=scroll_size, scroll_timeout=scroll_timeout
    )
    for hit in hits:
        source = hit.get("_source", {})
        try:
            batch_id = str(get_nested(source, batch_id_field))
        except KeyError:
            # A control doc without the join key cannot drive an extraction;
            # skip it rather than fail the whole feed.
            continue
        entry: dict[str, Any] = {
            "batch_id": batch_id,
            "message_id": hit.get("_id"),
        }
        if batch_size_field:
            try:
                entry["batch_size_intended"] = int(get_nested(source, batch_size_field))
            except (KeyError, TypeError, ValueError):
                pass
        batches[batch_id] = entry
    return list(batches.values())

from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


STEP_RETRIEVE = "retrieve_control_message"
STEP_EXTRACT = "extract_eod_message"
STEP_CONVERT = "convert_json_to_csv"
ALL_STEPS = (STEP_RETRIEVE, STEP_EXTRACT, STEP_CONVERT)

STATUS_DONE = "done"
STATUS_PENDING = "pending"
STATUS_FAILED = "failed"


def load_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as handle:
        state = json.load(handle)
    if not isinstance(state, dict):
        raise ValueError(f"Expected a JSON object in state file {path}")
    return state


def save_state(path: Path, state: dict[str, Any]) -> None:
    """Write the state atomically (temp file + rename in the same directory),
    so a crash mid-write never leaves a truncated state file — the previous
    run's state survives and the pipeline self-heals on the next run."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(dir=path.parent, prefix=path.name, suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(state, handle, indent=2, sort_keys=True)
            handle.write("\n")
        os.replace(tmp_name, path)
    except BaseException:
        try:
            os.unlink(tmp_name)
        except FileNotFoundError:
            pass
        raise


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def feed_entry(state: dict[str, Any], date_value: str, feed_name: str) -> dict[str, Any]:
    """Return (creating if absent) the mutable leaf for one (date, feed).

    Layout — outer key is the business date, nested key the feed name. Batches
    are tracked INDIVIDUALLY under 'batches' (keyed by batchId), so a late
    corrected batch shows up as an unseen batchId and is processed on the next
    run without touching batches already done:

        {
          "2026-02-05": {
            "facility": {
              "steps": {"retrieve_control_message": "done|pending|failed"},
              "log": "data/logs/facility-2026-02-05.log",
              "batches": {
                "C360_...": {
                  "steps": {
                    "extract_eod_message":  "done|pending|failed",
                    "convert_json_to_csv":  "done|pending|failed"
                  },
                  "json": "data/es_results/facility-C360_....jsonl",
                  "csv":  "data/csv/facility-2026-02-05-C360_....csv",
                  "record_count": 12345,
                  "batch_size_intended": 12345,
                  "updated_at": "2026-02-05T18:50:00Z"
                }
              },
              "updated_at": "2026-02-05T18:50:00Z"
            }
          }
        }
    """
    date_bucket = state.setdefault(date_value, {})
    entry = date_bucket.setdefault(feed_name, {})
    entry.setdefault("steps", {STEP_RETRIEVE: STATUS_PENDING})
    entry.setdefault("batches", {})
    return entry


def batch_entry(feed_state: dict[str, Any], batch_id: str) -> dict[str, Any]:
    """Return (creating if absent) the per-batchId leaf under a feed entry."""
    entry = feed_state["batches"].setdefault(batch_id, {"batch_id": batch_id})
    entry.setdefault(
        "steps", {STEP_EXTRACT: STATUS_PENDING, STEP_CONVERT: STATUS_PENDING}
    )
    return entry


def step_is_done(entry: dict[str, Any], step: str) -> bool:
    return entry.get("steps", {}).get(step) == STATUS_DONE


def mark_step(entry: dict[str, Any], step: str, status: str) -> None:
    """Record a step outcome. Callers must only pass STATUS_DONE after the step
    FULLY succeeded (file written, counts reconciled, exit code checked) — a
    crash before that leaves the step pending/failed, so the next cron run
    retries it automatically."""
    entry.setdefault("steps", {})[step] = status
    entry["updated_at"] = utc_now_iso()

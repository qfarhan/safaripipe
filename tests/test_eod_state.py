import json

from etl import eod_state
from etl.eod_state import (
    STATUS_DONE,
    STATUS_PENDING,
    STEP_CONVERT,
    STEP_EXTRACT,
    STEP_RETRIEVE,
    batch_entry,
    feed_entry,
    load_state,
    mark_step,
    save_state,
    step_is_done,
)


def test_load_state_missing_file_returns_empty(tmp_path):
    assert load_state(tmp_path / "nope.json") == {}


def test_save_and_load_state_roundtrip(tmp_path):
    path = tmp_path / "state" / "eod_state.json"
    state = {"2026-02-05": {"facility": {"steps": {STEP_RETRIEVE: STATUS_DONE}}}}
    save_state(path, state)
    assert load_state(path) == state
    # No leftover temp files from the atomic write.
    assert [p.name for p in path.parent.iterdir()] == [path.name]


def test_feed_entry_initializes_and_is_stable():
    state = {}
    entry = feed_entry(state, "2026-02-05", "facility")
    assert entry["steps"] == {STEP_RETRIEVE: STATUS_PENDING}
    assert entry["batches"] == {}
    entry["log"] = "somewhere.log"
    # A second call returns the SAME leaf, never resetting progress.
    assert feed_entry(state, "2026-02-05", "facility") is entry


def test_batch_entry_initializes_downstream_steps_pending():
    state = {}
    feed = feed_entry(state, "2026-02-05", "facility")
    batch = batch_entry(feed, "C360_A")
    assert batch["batch_id"] == "C360_A"
    assert batch["steps"] == {STEP_EXTRACT: STATUS_PENDING, STEP_CONVERT: STATUS_PENDING}
    batch["record_count"] = 7
    # Re-merging the same batchId (e.g. every 10-minute poll) keeps progress.
    assert batch_entry(feed, "C360_A") is batch
    assert batch_entry(feed, "C360_A")["record_count"] == 7


def test_mark_step_sets_status_and_timestamp():
    entry = {"steps": {}}
    mark_step(entry, STEP_EXTRACT, STATUS_DONE)
    assert step_is_done(entry, STEP_EXTRACT)
    assert entry["updated_at"].endswith("Z")


def test_state_file_is_valid_pretty_json(tmp_path):
    path = tmp_path / "eod_state.json"
    save_state(path, {"a": 1})
    raw = path.read_text(encoding="utf-8")
    assert json.loads(raw) == {"a": 1}
    assert raw.endswith("\n")

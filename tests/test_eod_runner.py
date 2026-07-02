import json
from datetime import date
from types import SimpleNamespace

import pytest

from etl import eod_runner
from etl.eod_runner import dates_window, run_eod, select_feeds
from etl.eod_state import (
    STATUS_DONE,
    STATUS_FAILED,
    STATUS_PENDING,
    STEP_CONVERT,
    STEP_EXTRACT,
    STEP_RETRIEVE,
)


RUN_DATE = date(2026, 2, 5)
FEED = {
    "name": "facility",
    "control_index": "ist.enterprise.c360.facility.eod.control.v1",
    "data_index": "ist.enterprise.c360.facility.eod.data.v1",
    "action_field": "control.action",
    "action_value": "End",
    "date_field": "control.eodDate",
    "batch_id_field": "header.batchId",
    "batch_size_field": "control.batchSizeIntended",
    "data_term_field": "header.batchId.keyword",
    "transform": ["python", "src/json_transform/transform_facility_to_csv.py"],
}


@pytest.fixture
def env(tmp_path, monkeypatch):
    """Fake config + collaborators. Returns a mutable harness the tests tune."""
    config = SimpleNamespace(
        env="test",
        elasticsearch={"hosts": ["http://localhost:9200"], "output_dir": str(tmp_path / "es_results")},
        eod={
            "state_file": str(tmp_path / "state" / "eod_state.json"),
            "log_dir": str(tmp_path / "logs"),
            "csv_dir": str(tmp_path / "csv"),
            "lookback_days": 0,
        },
        feeds=[dict(FEED)],
    )
    harness = SimpleNamespace(
        config=config,
        control_batches={},  # (control_index, date) -> list of batch dicts
        record_counts={},  # batch_id -> record_count returned by run_lookup
        lookup_calls=[],
        transform_calls=[],
        transform_returncode=0,
        state_path=tmp_path / "state" / "eod_state.json",
    )

    monkeypatch.setattr(eod_runner, "load_config", lambda env: config)
    monkeypatch.setattr(eod_runner, "create_es_client", lambda es_config: object())

    def fake_find(client, *, control_index, date_value, **kwargs):
        return [dict(b) for b in harness.control_batches.get((control_index, date_value), [])]

    monkeypatch.setattr(eod_runner, "find_completed_batches", fake_find)

    def fake_run_lookup(*, env, direct_id, index, term_field, output_file):
        harness.lookup_calls.append(
            {"id": direct_id, "index": index, "term_field": term_field, "output_file": output_file}
        )
        count = harness.record_counts.get(direct_id, 0)
        return {
            "output_file": output_file,
            "record_count": count,
            "total_matches": count,
        }

    monkeypatch.setattr(eod_runner, "run_lookup", fake_run_lookup)

    def fake_run_transform(command, json_path, csv_path):
        harness.transform_calls.append({"command": command, "json": json_path, "csv": csv_path})
        if harness.transform_returncode == 0:
            with open(csv_path, "w", encoding="utf-8") as handle:
                handle.write("BATCH_ID\n")
        return SimpleNamespace(returncode=harness.transform_returncode, stdout="", stderr="boom")

    monkeypatch.setattr(eod_runner, "run_transform", fake_run_transform)
    return harness


def _batch(batch_id, size=None):
    entry = {"batch_id": batch_id, "message_id": f"msg-{batch_id}"}
    if size is not None:
        entry["batch_size_intended"] = size
    return entry


def _feed_state(harness, date_value="2026-02-05"):
    state = json.loads(harness.state_path.read_text(encoding="utf-8"))
    return state[date_value]["facility"]


def test_dates_window_oldest_first():
    assert dates_window(RUN_DATE, 2) == ["2026-02-03", "2026-02-04", "2026-02-05"]
    assert dates_window(RUN_DATE, 0) == ["2026-02-05"]


def test_select_feeds_rejects_unknown_names():
    config = SimpleNamespace(feeds=[dict(FEED)])
    with pytest.raises(ValueError, match="Unknown feed"):
        select_feeds(config, ["nope"])


def test_happy_path_marks_all_steps_done(env):
    env.control_batches[(FEED["control_index"], "2026-02-05")] = [_batch("C360_A", 3)]
    env.record_counts["C360_A"] = 3

    summary = run_eod(env="test", run_date=RUN_DATE)

    assert summary["failures"] == []
    assert summary["warnings"] == []
    feed_state = _feed_state(env)
    assert feed_state["steps"][STEP_RETRIEVE] == STATUS_DONE
    batch = feed_state["batches"]["C360_A"]
    assert batch["steps"] == {STEP_EXTRACT: STATUS_DONE, STEP_CONVERT: STATUS_DONE}
    assert batch["record_count"] == 3
    assert batch["json"].endswith("facility-C360_A.jsonl")
    assert batch["csv"].endswith("facility-2026-02-05-C360_A.csv")
    # es_lookup was called with the FEED's index + keyword term field, not the
    # single [elasticsearch] defaults.
    assert env.lookup_calls == [
        {
            "id": "C360_A",
            "index": FEED["data_index"],
            "term_field": FEED["data_term_field"],
            "output_file": batch["json"],
        }
    ]


def test_feed_not_ready_is_noop_not_failure(env):
    summary = run_eod(env="test", run_date=RUN_DATE)
    assert summary["failures"] == []
    assert summary["warnings"] == []  # today may legitimately still be pending
    assert _feed_state(env)["steps"][STEP_RETRIEVE] == STATUS_PENDING


def test_second_run_skips_done_steps(env):
    env.control_batches[(FEED["control_index"], "2026-02-05")] = [_batch("C360_A", 3)]
    env.record_counts["C360_A"] = 3
    run_eod(env="test", run_date=RUN_DATE)
    run_eod(env="test", run_date=RUN_DATE)
    # extract + convert ran exactly once; the second run skipped them.
    assert len(env.lookup_calls) == 1
    assert len(env.transform_calls) == 1


def test_force_reruns_done_steps(env):
    env.control_batches[(FEED["control_index"], "2026-02-05")] = [_batch("C360_A", 3)]
    env.record_counts["C360_A"] = 3
    run_eod(env="test", run_date=RUN_DATE)
    run_eod(env="test", run_date=RUN_DATE, force=True)
    assert len(env.lookup_calls) == 2
    assert len(env.transform_calls) == 2


def test_late_corrected_batch_is_picked_up(env):
    # Run 1 processes batch A completely.
    env.control_batches[(FEED["control_index"], "2026-02-05")] = [_batch("C360_A", 3)]
    env.record_counts["C360_A"] = 3
    run_eod(env="test", run_date=RUN_DATE)

    # A corrected batch B appears later for the SAME date: only B is processed,
    # A's done steps are untouched.
    env.control_batches[(FEED["control_index"], "2026-02-05")].append(_batch("C360_B", 5))
    env.record_counts["C360_B"] = 5
    summary = run_eod(env="test", run_date=RUN_DATE)

    assert summary["failures"] == []
    assert [call["id"] for call in env.lookup_calls] == ["C360_A", "C360_B"]
    batches = _feed_state(env)["batches"]
    assert batches["C360_A"]["steps"][STEP_CONVERT] == STATUS_DONE
    assert batches["C360_B"]["steps"][STEP_CONVERT] == STATUS_DONE


def test_batch_size_mismatch_fails_extract_and_retries(env):
    env.control_batches[(FEED["control_index"], "2026-02-05")] = [_batch("C360_A", 10)]
    env.record_counts["C360_A"] = 7  # data index not fully landed yet

    summary = run_eod(env="test", run_date=RUN_DATE)
    batch = _feed_state(env)["batches"]["C360_A"]
    assert batch["steps"][STEP_EXTRACT] == STATUS_FAILED
    assert batch["steps"][STEP_CONVERT] == STATUS_PENDING  # never attempted
    assert env.transform_calls == []
    assert any("extract_eod_message failed" in failure for failure in summary["failures"])

    # Next cron run: the full batch has landed — extract retries WITHOUT --force
    # and the pipeline completes (self-healing).
    env.record_counts["C360_A"] = 10
    summary = run_eod(env="test", run_date=RUN_DATE)
    assert summary["failures"] == []
    assert _feed_state(env)["batches"]["C360_A"]["steps"][STEP_CONVERT] == STATUS_DONE


def test_transform_failure_marks_convert_failed_then_retries(env):
    env.control_batches[(FEED["control_index"], "2026-02-05")] = [_batch("C360_A", 3)]
    env.record_counts["C360_A"] = 3
    env.transform_returncode = 1

    summary = run_eod(env="test", run_date=RUN_DATE)
    batch = _feed_state(env)["batches"]["C360_A"]
    assert batch["steps"][STEP_EXTRACT] == STATUS_DONE
    assert batch["steps"][STEP_CONVERT] == STATUS_FAILED
    assert summary["failures"]

    # Fix the transform; only convert reruns (extract stays done).
    env.transform_returncode = 0
    summary = run_eod(env="test", run_date=RUN_DATE)
    assert summary["failures"] == []
    assert len(env.lookup_calls) == 1
    assert len(env.transform_calls) == 2


def test_lookback_flags_missing_past_date(env):
    # Yesterday never produced an End batch; today did. The past date inside
    # the lookback window must surface as a MISSING warning, not vanish.
    env.control_batches[(FEED["control_index"], "2026-02-05")] = [_batch("C360_A", 1)]
    env.record_counts["C360_A"] = 1

    summary = run_eod(env="test", run_date=RUN_DATE, lookback_days=1)

    assert summary["dates"] == ["2026-02-04", "2026-02-05"]
    assert any("MISSING" in w and "2026-02-04" in w for w in summary["warnings"])
    assert summary["failures"] == []


def test_lookback_processes_late_previous_day(env):
    # Yesterday's End arrives late: the lookback window picks it up even though
    # --date already rolled to today.
    env.control_batches[(FEED["control_index"], "2026-02-04")] = [_batch("C360_OLD", 2)]
    env.record_counts["C360_OLD"] = 2

    summary = run_eod(env="test", run_date=RUN_DATE, lookback_days=1)

    assert summary["failures"] == []
    assert _feed_state(env, "2026-02-04")["batches"]["C360_OLD"]["steps"][STEP_CONVERT] == STATUS_DONE


def test_steps_subset_runs_only_convert(env, monkeypatch):
    env.control_batches[(FEED["control_index"], "2026-02-05")] = [_batch("C360_A", 3)]
    env.record_counts["C360_A"] = 3
    run_eod(env="test", run_date=RUN_DATE, steps=[STEP_RETRIEVE, STEP_EXTRACT])
    assert env.transform_calls == []

    # ES must not be needed for a convert-only run.
    def boom(es_config):
        raise AssertionError("ES client must not be created for convert-only runs")

    monkeypatch.setattr(eod_runner, "create_es_client", boom)
    summary = run_eod(env="test", run_date=RUN_DATE, steps=[STEP_CONVERT])
    assert summary["failures"] == []
    assert len(env.transform_calls) == 1
    assert _feed_state(env)["batches"]["C360_A"]["steps"][STEP_CONVERT] == STATUS_DONE


def test_unknown_step_rejected(env):
    with pytest.raises(ValueError, match="Unknown step"):
        run_eod(env="test", run_date=RUN_DATE, steps=["frobnicate"])


def test_control_query_failure_marks_retrieve_failed(env, monkeypatch):
    def boom(client, **kwargs):
        raise ConnectionError("ES down")

    monkeypatch.setattr(eod_runner, "find_completed_batches", boom)
    summary = run_eod(env="test", run_date=RUN_DATE)
    assert any(STEP_RETRIEVE in failure for failure in summary["failures"])
    assert _feed_state(env)["steps"][STEP_RETRIEVE] == STATUS_FAILED


def test_writes_per_feed_date_log_file(env):
    env.control_batches[(FEED["control_index"], "2026-02-05")] = [_batch("C360_A", 1)]
    env.record_counts["C360_A"] = 1
    run_eod(env="test", run_date=RUN_DATE)
    feed_state = _feed_state(env)
    log_path = feed_state["log"]
    assert log_path.endswith("facility-2026-02-05.log")
    with open(log_path, encoding="utf-8") as handle:
        content = handle.read()
    assert "C360_A" in content

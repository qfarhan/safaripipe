from __future__ import annotations

import argparse
import json
import logging
import subprocess
import sys
from datetime import date as date_type, timedelta
from pathlib import Path
from typing import Any

from .config import PROJECT_ROOT, load_config, resolve_project_path
from .control_query import find_completed_batches
from .es_lookup import create_es_client, run_lookup
from .eod_state import (
    ALL_STEPS,
    STATUS_DONE,
    STATUS_FAILED,
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


DEFAULT_STATE_FILE = "data/state/eod_state.json"
DEFAULT_LOG_DIR = "data/logs"
DEFAULT_CSV_DIR = "data/csv"
DEFAULT_LOOKBACK_DAYS = 0


def safe_name(value: str) -> str:
    return str(value).replace("/", "_")


def dates_window(end_date: date_type, lookback_days: int) -> list[str]:
    """Business dates to consider, oldest first: end_date and the lookback_days
    before it. Re-checking recent past dates lets a feed whose End arrived late
    (or a corrected batch with a new batchId) self-heal on a later cron run."""
    days = max(int(lookback_days), 0)
    return [
        (end_date - timedelta(days=offset)).isoformat()
        for offset in range(days, -1, -1)
    ]


def feed_logger(feed_name: str, date_value: str, log_dir: Path) -> logging.Logger:
    """Per-(feed, date) logger writing to data/logs/<feed>-<date>.log and echoing
    to stderr. Handlers are replaced on every call so repeated runs in one
    process do not stack duplicate handlers."""
    log_dir.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger(f"eod.{feed_name}.{date_value}")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    for handler in list(logger.handlers):
        logger.removeHandler(handler)
        handler.close()
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    file_handler = logging.FileHandler(log_dir / f"{feed_name}-{date_value}.log", encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    console = logging.StreamHandler(sys.stderr)
    console.setFormatter(logging.Formatter(f"[{feed_name} {date_value}] %(levelname)s %(message)s"))
    logger.addHandler(console)
    return logger


def close_logger(logger: logging.Logger) -> None:
    for handler in list(logger.handlers):
        logger.removeHandler(handler)
        handler.close()


def run_transform(command: list[str], json_path: str, csv_path: str) -> subprocess.CompletedProcess:
    """Invoke the per-feed transform script: <command...> <input.jsonl> <output.csv>.
    Runs from the project root so config-relative commands like
    ["python", "src/json_transform/transform_facility_to_csv.py"] resolve."""
    return subprocess.run(
        [*command, json_path, csv_path],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
    )


def select_feeds(config: Any, feed_names: list[str] | None) -> list[dict[str, Any]]:
    if not config.feeds:
        raise ValueError(
            "No [[feeds]] configured. Add at least one feed to the config file."
        )
    if not feed_names:
        return list(config.feeds)
    by_name = {str(feed.get("name")): feed for feed in config.feeds}
    missing = [name for name in feed_names if name not in by_name]
    if missing:
        raise ValueError(
            f"Unknown feed(s): {', '.join(missing)}. "
            f"Configured feeds: {', '.join(sorted(by_name))}"
        )
    return [by_name[name] for name in feed_names]


def _retrieve_step(
    *,
    client_factory,
    config,
    feed: dict[str, Any],
    date_value: str,
    feed_state: dict[str, Any],
    logger: logging.Logger,
) -> None:
    """Query the feed's control index for completed (action=End) batches on
    date_value and merge every batchId into state.

    This step re-runs on every invocation even when previously done: it is the
    polling step, and re-querying is what detects a late or corrected batch
    (a new batchId) after the date was already processed. Batches already in
    state keep their step statuses — merging never resets downstream progress.
    """
    es_config = config.elasticsearch
    try:
        client = client_factory()
        found = find_completed_batches(
            client,
            control_index=str(feed["control_index"]),
            action_field=str(feed["action_field"]),
            action_value=str(feed["action_value"]),
            date_field=str(feed["date_field"]),
            date_value=date_value,
            batch_id_field=str(feed["batch_id_field"]),
            batch_size_field=str(feed.get("batch_size_field", "")) or None,
            scroll_size=int(es_config.get("scroll_size", 1000)),
            scroll_timeout=str(es_config.get("scroll_timeout", "2m")),
        )
    except Exception:
        logger.exception("control query failed for index %s", feed.get("control_index"))
        mark_step(feed_state, STEP_RETRIEVE, STATUS_FAILED)
        return

    for batch in found:
        entry = batch_entry(feed_state, batch["batch_id"])
        if "message_id" in batch:
            entry["message_id"] = batch["message_id"]
        if "batch_size_intended" in batch:
            entry["batch_size_intended"] = batch["batch_size_intended"]

    if feed_state["batches"]:
        mark_step(feed_state, STEP_RETRIEVE, STATUS_DONE)
        logger.info(
            "control query found %d batch(es): %s",
            len(found),
            ", ".join(sorted(feed_state["batches"])),
        )
    else:
        # No End yet: the feed is simply not ready. Leave the step pending
        # (NOT failed) so the next cron run retries without --force.
        mark_step(feed_state, STEP_RETRIEVE, STATUS_PENDING)
        logger.info("no completed batches yet (action=%s)", feed.get("action_value"))


def _extract_step(
    *,
    env: str,
    feed: dict[str, Any],
    batch: dict[str, Any],
    output_dir: Path,
    force: bool,
    logger: logging.Logger,
) -> None:
    batch_id = batch["batch_id"]
    if step_is_done(batch, STEP_EXTRACT) and not force:
        logger.info("extract already done for batch %s (skip)", batch_id)
        return
    feed_name = str(feed["name"])
    destination = output_dir / f"{feed_name}-{safe_name(batch_id)}.jsonl"
    try:
        result = run_lookup(
            env=env,
            direct_id=batch_id,
            index=str(feed["data_index"]),
            term_field=str(feed["data_term_field"]),
            output_file=str(destination),
        )
    except Exception:
        logger.exception("extract failed for batch %s", batch_id)
        mark_step(batch, STEP_EXTRACT, STATUS_FAILED)
        return

    batch["json"] = result["output_file"]
    batch["record_count"] = result["record_count"]
    batch["total_matches"] = result["total_matches"]

    if result.get("warning"):
        # The scroll was cut short: the JSONL on disk is incomplete. Mark
        # failed so the next run redoes the extraction from scratch.
        logger.error("extract incomplete for batch %s: %s", batch_id, result["warning"])
        mark_step(batch, STEP_EXTRACT, STATUS_FAILED)
        return

    expected = batch.get("batch_size_intended")
    if expected is not None and int(expected) != int(result["record_count"]):
        # The control doc promised batchSizeIntended records; extracting a
        # different count means the data index has not fully landed (or the
        # term query is off). Retry on the next run instead of converting a
        # partial batch.
        logger.error(
            "extract count mismatch for batch %s: batchSizeIntended=%s but record_count=%s",
            batch_id,
            expected,
            result["record_count"],
        )
        mark_step(batch, STEP_EXTRACT, STATUS_FAILED)
        return

    mark_step(batch, STEP_EXTRACT, STATUS_DONE)
    logger.info(
        "extracted %d record(s) for batch %s -> %s",
        result["record_count"],
        batch_id,
        result["output_file"],
    )


def _convert_step(
    *,
    feed: dict[str, Any],
    date_value: str,
    batch: dict[str, Any],
    csv_dir: Path,
    force: bool,
    logger: logging.Logger,
) -> None:
    batch_id = batch["batch_id"]
    if not step_is_done(batch, STEP_EXTRACT):
        logger.info("convert waiting on extract for batch %s (skip)", batch_id)
        return
    if step_is_done(batch, STEP_CONVERT) and not force:
        logger.info("convert already done for batch %s (skip)", batch_id)
        return
    command = feed.get("transform")
    if not command:
        logger.error("feed %s has no transform command configured", feed.get("name"))
        mark_step(batch, STEP_CONVERT, STATUS_FAILED)
        return

    csv_dir.mkdir(parents=True, exist_ok=True)
    # The batchId is part of the CSV name so two distinct End batches on the
    # same date never clobber each other; re-running the SAME batch overwrites
    # its own file, which is the safe-reprocessing behavior we want.
    csv_path = csv_dir / f"{feed['name']}-{date_value}-{safe_name(batch_id)}.csv"
    try:
        completed = run_transform([str(part) for part in command], str(batch["json"]), str(csv_path))
    except Exception:
        logger.exception("transform invocation failed for batch %s", batch_id)
        mark_step(batch, STEP_CONVERT, STATUS_FAILED)
        return

    if completed.stdout:
        logger.info("transform output for batch %s:\n%s", batch_id, completed.stdout.strip())
    if completed.returncode != 0:
        logger.error(
            "transform failed for batch %s (exit %d):\n%s",
            batch_id,
            completed.returncode,
            (completed.stderr or "").strip(),
        )
        mark_step(batch, STEP_CONVERT, STATUS_FAILED)
        return
    if not csv_path.exists():
        logger.error("transform exited 0 but wrote no CSV at %s", csv_path)
        mark_step(batch, STEP_CONVERT, STATUS_FAILED)
        return

    batch["csv"] = str(csv_path)
    mark_step(batch, STEP_CONVERT, STATUS_DONE)
    logger.info("converted batch %s -> %s", batch_id, csv_path)


def run_eod(
    *,
    env: str,
    feed_names: list[str] | None = None,
    run_date: date_type | None = None,
    steps: list[str] | None = None,
    force: bool = False,
    lookback_days: int | None = None,
) -> dict[str, Any]:
    config = load_config(env)
    eod = config.eod
    feeds = select_feeds(config, feed_names)
    selected_steps = list(steps or ALL_STEPS)
    for step in selected_steps:
        if step not in ALL_STEPS:
            raise ValueError(f"Unknown step '{step}'. Valid steps: {', '.join(ALL_STEPS)}")

    end_date = run_date or date_type.today()
    if lookback_days is None:
        lookback_days = int(eod.get("lookback_days", DEFAULT_LOOKBACK_DAYS))
    window = dates_window(end_date, lookback_days)

    state_path = resolve_project_path(eod.get("state_file", DEFAULT_STATE_FILE))
    log_dir = resolve_project_path(eod.get("log_dir", DEFAULT_LOG_DIR))
    csv_dir = resolve_project_path(eod.get("csv_dir", DEFAULT_CSV_DIR))
    output_dir = resolve_project_path(config.elasticsearch.get("output_dir", "data/es_results"))
    state = load_state(state_path)

    # One client for the whole run (shared [elasticsearch] connection), created
    # lazily so runs that skip retrieve_control_message never need ES up.
    client_cache: dict[str, Any] = {}

    def client_factory():
        if "client" not in client_cache:
            client_cache["client"] = create_es_client(config.elasticsearch)
        return client_cache["client"]

    warnings: list[str] = []
    failures: list[str] = []

    for date_value in window:
        for feed in feeds:
            feed_name = str(feed["name"])
            logger = feed_logger(feed_name, date_value, log_dir)
            try:
                feed_state = feed_entry(state, date_value, feed_name)
                feed_state["log"] = str(log_dir / f"{feed_name}-{date_value}.log")

                if STEP_RETRIEVE in selected_steps:
                    _retrieve_step(
                        client_factory=client_factory,
                        config=config,
                        feed=feed,
                        date_value=date_value,
                        feed_state=feed_state,
                        logger=logger,
                    )
                    save_state(state_path, state)

                if not feed_state["batches"] and date_value != window[-1]:
                    # A PAST date inside the lookback window still has no End
                    # batch. Surface it: once the date ages out of the window it
                    # would otherwise be skipped silently forever.
                    message = (
                        f"MISSING: feed '{feed_name}' has no completed (End) batch "
                        f"for {date_value}"
                    )
                    warnings.append(message)
                    logger.warning(message)

                for batch in list(feed_state["batches"].values()):
                    if STEP_EXTRACT in selected_steps:
                        _extract_step(
                            env=env,
                            feed=feed,
                            batch=batch,
                            output_dir=output_dir,
                            force=force,
                            logger=logger,
                        )
                        save_state(state_path, state)
                    if STEP_CONVERT in selected_steps:
                        _convert_step(
                            feed=feed,
                            date_value=date_value,
                            batch=batch,
                            csv_dir=csv_dir,
                            force=force,
                            logger=logger,
                        )
                        save_state(state_path, state)

                for batch in feed_state["batches"].values():
                    for step, status in batch.get("steps", {}).items():
                        if status == STATUS_FAILED:
                            failures.append(
                                f"{feed_name} {date_value} batch {batch['batch_id']}: {step} failed"
                            )
                if feed_state["steps"].get(STEP_RETRIEVE) == STATUS_FAILED:
                    failures.append(f"{feed_name} {date_value}: {STEP_RETRIEVE} failed")
            finally:
                close_logger(logger)

    save_state(state_path, state)
    return {
        "env": env,
        "dates": window,
        "feeds": [str(feed["name"]) for feed in feeds],
        "steps": selected_steps,
        "force": force,
        "state_file": str(state_path),
        "warnings": warnings,
        "failures": failures,
        "state": {date_value: state.get(date_value, {}) for date_value in window},
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Scheduled EOD pipeline: query each feed's ES control index for the "
            "day's completed batch, extract the full batch from the data index, "
            "and convert it to CSV. Designed to run from cron every ~10 minutes; "
            "steps already marked done in the state file are skipped."
        )
    )
    parser.add_argument("--env", default="local", help="Config environment name, e.g. local/dev/prod.")
    parser.add_argument(
        "--feed",
        action="append",
        dest="feeds",
        metavar="NAME",
        help="Feed name to process (repeatable). Default: every feed in [[feeds]].",
    )
    parser.add_argument(
        "--date",
        help="Business date YYYY-MM-DD. Default: today. The lookback window extends backwards from this date.",
    )
    parser.add_argument(
        "--steps",
        help=f"Comma-separated subset of: {','.join(ALL_STEPS)}. Default: all three, in order.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-run steps already marked done in the state file.",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=None,
        metavar="N",
        help="Also re-check the N days before --date (default: [eod].lookback_days from config).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    run_date = date_type.fromisoformat(args.date) if args.date else None
    steps = [step.strip() for step in args.steps.split(",") if step.strip()] if args.steps else None
    summary = run_eod(
        env=args.env,
        feed_names=args.feeds,
        run_date=run_date,
        steps=steps,
        force=args.force,
        lookback_days=args.lookback_days,
    )
    print(json.dumps(summary, indent=2, sort_keys=True))
    # Non-zero exit when a step failed, so cron/monitoring notices. A feed that
    # is merely not ready yet (no End batch today) is NOT a failure.
    return 1 if summary["failures"] else 0


if __name__ == "__main__":
    raise SystemExit(main())

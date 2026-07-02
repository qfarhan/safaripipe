#!/usr/bin/env bash
# Cron entry point for the scheduled EOD pipeline. All etl.eod_runner flags
# pass through, e.g.:
#   ./scripts/run_eod.sh --env dev
#   ./scripts/run_eod.sh --env dev --feed facility --date 2026-02-05 --steps convert_json_to_csv --force
#
# Example crontab (every 10 minutes):
#   */10 * * * * /path/to/kafka-codex/scripts/run_eod.sh --env prod >> /path/to/kafka-codex/data/logs/cron.log 2>&1
set -euo pipefail

cd "$(dirname "$0")/.."
# shellcheck disable=SC1091
[ -f .venv/bin/activate ] && source .venv/bin/activate
PYTHONPATH="${PYTHONPATH:-}:src" python -m etl.eod_runner "$@"

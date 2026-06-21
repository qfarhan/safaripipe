#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
PYTHONPATH="${PYTHONPATH:-}:src" python -m etl.kafka_consumer \
  --env local \
  --message-file samples/control_message.json \
  --trigger-next \
  --dry-run-next

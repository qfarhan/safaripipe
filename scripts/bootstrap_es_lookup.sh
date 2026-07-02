#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
PYTHONPATH="${PYTHONPATH:-}:src" python -m etl.es_lookup \
  --env local \
  --json '{"header":{"batchId":"customer-123"},"event_type":"manual.debug"}' \
  --dry-run

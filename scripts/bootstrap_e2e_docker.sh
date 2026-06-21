#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

PYTHON="${PYTHON:-.venv/bin/python}"
EVENT_ID="${EVENT_ID:-customer-123}"

for _ in {1..60}; do
  if curl -fsS "http://localhost:9200" >/dev/null; then
    break
  fi
  sleep 2
done

curl -fsS -X PUT "http://localhost:9200/source-index" \
  -H "Content-Type: application/json" \
  -d '{"mappings":{"properties":{"event_id":{"type":"keyword"},"name":{"type":"text"},"updated_at":{"type":"date"}}}}' >/dev/null || true

curl -fsS -X POST "http://localhost:9200/source-index/_doc/${EVENT_ID}?refresh=true" \
  -H "Content-Type: application/json" \
  -d "{\"event_id\":\"${EVENT_ID}\",\"name\":\"Demo Customer\",\"updated_at\":\"2026-06-21T00:00:00Z\"}" >/dev/null

"${PYTHON}" -m etl.kafka_producer \
  --env local \
  --message-file samples/control_message.json \
  --key "${EVENT_ID}"

"${PYTHON}" -m etl.kafka_consumer \
  --env local \
  --once \
  --trigger-next

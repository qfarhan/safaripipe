#!/usr/bin/env bash
# Live end-to-end test over PLAINTEXT with Avro + Schema Registry:
#   seed ES  ->  publish an Avro control message  ->  consume one + convert + save
#   ->  trigger the live Elasticsearch lookup.
# Requires: docker compose up -d   (kafka + schema-registry + elasticsearch)
set -euo pipefail

cd "$(dirname "$0")/.."

PYTHON="${PYTHON:-.venv/bin/python}"
EVENT_ID="${EVENT_ID:-customer-123}"

# Wait for Elasticsearch.
for _ in {1..60}; do
  if curl -fsS "http://localhost:9200" >/dev/null; then break; fi
  sleep 2
done

# Wait for Schema Registry (needed by the Avro producer + consumer).
for _ in {1..60}; do
  if curl -fsS "http://localhost:8081/subjects" >/dev/null; then break; fi
  sleep 2
done

curl -fsS -X PUT "http://localhost:9200/source-index" \
  -H "Content-Type: application/json" \
  -d '{"mappings":{"properties":{"event_id":{"type":"keyword"},"name":{"type":"text"},"updated_at":{"type":"date"}}}}' >/dev/null || true

curl -fsS -X POST "http://localhost:9200/source-index/_doc/${EVENT_ID}?refresh=true" \
  -H "Content-Type: application/json" \
  -d "{\"event_id\":\"${EVENT_ID}\",\"name\":\"Demo Customer\",\"updated_at\":\"2026-06-21T00:00:00Z\"}" >/dev/null

# Publish the sample message, Avro-encoded and registered under <topic>-value.
"${PYTHON}" -m etl.kafka_producer \
  --env local \
  --message-file samples/control_message.json \
  --key "${EVENT_ID}"

# Consume one Avro batch, deserialize via Schema Registry, convert, save, and
# trigger the live Elasticsearch lookup.
"${PYTHON}" -m etl.kafka_consumer \
  --env local \
  --once \
  --trigger-next

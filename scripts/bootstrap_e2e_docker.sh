#!/usr/bin/env bash
# Live end-to-end test over PLAINTEXT with Avro + Schema Registry:
#   seed ES  ->  publish two Avro control messages (Start + End)  ->  consume both;
#   only the End message clears the save-gate (control.batch.processStatus == "End"),
#   gets converted + saved, and triggers the live Elasticsearch lookup keyed on
#   header.batchId.
# Requires: docker compose up -d   (kafka + schema-registry + elasticsearch)
set -euo pipefail

cd "$(dirname "$0")/.."

PYTHON="${PYTHON:-.venv/bin/python}"
# header.batchId of samples/control_message.json (the "End" message we expect to save).
BATCH_ID="${BATCH_ID:-batch-2026-06-29-001}"

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

# Recreate the index so the intended mapping always applies. (If the index already
# exists, a PUT is rejected and ES would otherwise dynamically map header.batchId as
# `text`, which a term query cannot match on a hyphenated id.)
curl -fsS -X DELETE "http://localhost:9200/source-index" >/dev/null 2>&1 || true

# header is an ES object; header.batchId is a keyword so the term query matches exactly.
curl -fsS -X PUT "http://localhost:9200/source-index" \
  -H "Content-Type: application/json" \
  -d '{"mappings":{"properties":{"header":{"properties":{"batchId":{"type":"keyword"}}},"name":{"type":"text"},"updated_at":{"type":"date"}}}}' >/dev/null

curl -fsS -X POST "http://localhost:9200/source-index/_doc/${BATCH_ID}?refresh=true" \
  -H "Content-Type: application/json" \
  -d "{\"header\":{\"batchId\":\"${BATCH_ID}\"},\"name\":\"Demo Batch\",\"updated_at\":\"2026-06-21T00:00:00Z\"}" >/dev/null

# Publish a "Start" message first — the consumer should SKIP it (gate not cleared).
"${PYTHON}" -m etl.kafka_producer \
  --env local \
  --message-file samples/control_message_start.json \
  --key "batch-2026-06-29-002"

# Publish the "End" message — this one is Avro-encoded, registered under
# <topic>-value, and is the one we expect to be saved + looked up.
"${PYTHON}" -m etl.kafka_producer \
  --env local \
  --message-file samples/control_message.json \
  --key "${BATCH_ID}"

# Consume both Avro batches, deserialize via Schema Registry, apply the save-gate,
# convert + save the End message, and trigger the live Elasticsearch lookup.
"${PYTHON}" -m etl.kafka_consumer \
  --env local \
  --once \
  --trigger-next

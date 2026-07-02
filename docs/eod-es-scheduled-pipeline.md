# Scheduled EOD Pipeline (Elasticsearch control index)

The EOD "batch complete" signal does not only exist in Kafka: the source system
also writes it to an independent Elasticsearch **control index**. This pipeline
polls that control index on a schedule (cron, ~every 10 minutes) instead of
consuming the Kafka control topic, then reuses the existing extraction and
transform machinery:

```
cron (every ~10 min)
  └─ scripts/run_eod.sh  →  python -m etl.eod_runner
       for each [[feeds]] entry, for each date in the lookback window:
         1. retrieve_control_message   query the feed's CONTROL index for
            (etl.control_query)        action=End on the business date → batchId(s)
         2. extract_eod_message        term-query the feed's DATA index for each
            (etl.es_lookup)            batchId, scroll ALL hits → data/es_results/*.jsonl
         3. convert_json_to_csv        feed's own transform script
            (src/json_transform/)      → data/csv/*.csv
```

The Kafka consumer path (`etl.kafka_consumer`) is **kept unchanged** as a
fallback/testing path; nothing here removes it. The tutorial docs still
describe the Kafka-triggered pipeline — this document covers only the
scheduled ES path.

## Why a control index query instead of Kafka

A control document (example alias `ist.enterprise.c360.facility.eod.control.v1`,
an alias over `...v1-r1`) already contains everything the trigger needs:

| Field                       | Meaning                                        |
|-----------------------------|------------------------------------------------|
| `header.batchId`            | join key into the DATA index                   |
| `header.messageId` (= `_id`)| globally unique message id                     |
| `control.action`            | `"Start"` / `"End"` — `End` marks completion   |
| `control.eodDate`           | business date, e.g. `2026-02-05`               |
| `control.batchSizeIntended` | expected record count for the batch            |

Note the real control index uses `control.action` / `control.eodDate`, while
the sample Avro schema on the Kafka path uses `control.batch.processStatus`.
Field paths therefore live **per feed in config** — never hardcoded.

## Configuration (`config/<env>.toml`)

Static configuration is version-controlled; runtime state is not (see State
below). The shared `[elasticsearch]` block supplies the connection (hosts,
auth, scroll tuning); each feed adds only what differs:

```toml
[eod]
state_file = "data/state/eod_state.json"
log_dir = "data/logs"
csv_dir = "data/csv"
lookback_days = 3

[[feeds]]
name = "facility"
control_index = "ist.enterprise.c360.facility.eod.control.v1"  # the ALIAS, never -r1
data_index = "ist.enterprise.c360.facility.eod.data.v1"
action_field = "control.action"          # keyword field (append .keyword if text)
action_value = "End"
date_field = "control.eodDate"           # date (or keyword) field
batch_id_field = "header.batchId"        # dotted path inside the control doc _source
batch_size_field = "control.batchSizeIntended"   # optional record-count assertion
data_term_field = "header.batchId.keyword"       # term-query field in the DATA index
transform = ["python", "src/json_transform/transform_facility_to_csv.py"]
```

### Correctness rules baked into the control query (`etl.control_query`)

- **term queries in filter context**, never `match`: the fields are
  keyword/date, matching must be exact and unscored. A `term` query against a
  `text`-mapped field silently matches nothing (`"End"` vs the analyzed token
  `"end"`) — if the mapping is text-with-subfield, point `action_field` at
  `control.action.keyword`.
- **query the alias**, not the versioned `-r1` index behind it, so a
  reindex/rollover on the source side is transparent.
- **page every hit** with the same scroll helper `etl.es_lookup` uses: a busy
  day can produce more control docs than the default 10-hit page.
- multiple `End` docs for one date are all honored (every distinct batchId is
  processed); duplicate docs for the *same* batchId collapse to one entry with
  the last doc's `batchSizeIntended` winning.

## Runner (`etl.eod_runner` / `scripts/run_eod.sh`)

```
./scripts/run_eod.sh [--env ENV] [--feed NAME]... [--date YYYY-MM-DD]
                     [--steps s1,s2] [--force] [--lookback-days N]
```

| Flag             | Default                        | Meaning                                   |
|------------------|--------------------------------|-------------------------------------------|
| `--feed`         | all `[[feeds]]`                | repeatable feed selector                   |
| `--date`         | today                          | business date; window extends backwards    |
| `--steps`        | all three, in order            | comma list; any subset runs independently  |
| `--force`        | off                            | re-run steps already marked done           |
| `--lookback-days`| `[eod].lookback_days`          | also re-check the N days before `--date`   |

Steps are **independently resumable**: each is keyed by (feed, date, batchId)
in the state file, so e.g. after fixing a transform you can re-run only the
conversion for one feed and date:

```
./scripts/run_eod.sh --env dev --feed facility --date 2026-02-05 \
    --steps convert_json_to_csv --force
```

Behavior per (feed, date):

- **No `End` doc yet** → the feed is simply not ready: the step stays
  `pending` (not failed), the run exits 0, and the next cron run retries.
- **A step is marked `done` only after it fully succeeds** (file written,
  counts reconciled, transform exit code checked). A crash mid-pipeline leaves
  the step `pending`/`failed`, so the next run self-heals.
- **Reprocessing is safe**: re-running a batch overwrites its own JSONL/CSV.
  Idempotency (the done-skip) is an optimization, not a correctness gate.
- The extraction reuses `es_lookup.run_lookup` with per-feed
  `index`/`term_field` overrides and inherits its reconciliation: a scroll
  shortfall (`record_count != total_matches`) fails the step, and when the
  control doc carries `batchSizeIntended`, a count mismatch also fails the
  step (the data index has not fully landed yet) — both retry next run.
- Exit code is non-zero when any step **failed**, so cron/monitoring notices.

## State (`data/state/eod_state.json`, gitignored)

Runtime, mutable state — kept strictly out of config. Outer key = business
date, nested key = feed; batches are tracked **individually by batchId**, so a
late corrected batch (a new batchId for an already-processed date, within the
lookback window) is picked up automatically without touching finished batches:

```json
{
  "2026-02-05": {
    "facility": {
      "steps": { "retrieve_control_message": "done" },
      "log": "data/logs/facility-2026-02-05.log",
      "batches": {
        "C360_20260205183000_...": {
          "batch_id": "C360_20260205183000_...",
          "message_id": "msg-end-1",
          "batch_size_intended": 12345,
          "steps": {
            "extract_eod_message": "done",
            "convert_json_to_csv": "done"
          },
          "json": "data/es_results/facility-C360_20260205183000_....jsonl",
          "csv": "data/csv/facility-2026-02-05-C360_20260205183000_....csv",
          "record_count": 12345,
          "total_matches": 12345,
          "updated_at": "2026-02-05T18:50:00Z"
        }
      },
      "updated_at": "2026-02-05T18:50:00Z"
    }
  }
}
```

`retrieve_control_message` re-runs on every invocation even when previously
done — it is the polling step, and re-querying is what detects late/corrected
batches. Merging never resets downstream progress. The state file is written
atomically (temp file + rename) after every step.

### Lookback window and MISSING alerts

Each run processes `--date` plus the `lookback_days` before it. A **past**
date inside the window that still has no `End` batch is reported as a
`MISSING: ...` warning in the run summary (and the feed's log) on every run —
otherwise a feed that never signals completion would be skipped silently once
the date rolls over. The current date is allowed to be pending without a
warning (the day is not over).

Logs are written per (feed, date) to `data/logs/<feed>-<date>.log`; the path
is recorded in state.

## Per-feed transforms (`src/json_transform/`)

Each feed owns a `transform_<feed>_to_csv.py` plus a `<feed>_fields.yaml`,
modeled on the `json_to_csv.py` + `fields.yaml` template. The runner invokes
the configured command as `<command...> <input.jsonl> <output.csv>` from the
project root and requires exit code 0 *and* an existing CSV before marking the
step done. Feed-specific logic (derived columns, reference-data lookups)
belongs in the feed's script; shared plumbing (`get_path`, `coerce`) stays in
`json_to_csv.py`.

`facility_fields.yaml` is a **template**: its dotted paths must be verified
against a real batch before production use.

### Adding a new feed

1. Copy `transform_facility_to_csv.py` → `transform_<feed>_to_csv.py` and
   `facility_fields.yaml` → `<feed>_fields.yaml`; adjust the field spec.
2. Add a `[[feeds]]` entry with the feed's control/data aliases, field paths,
   and transform command.
3. Verify the mappings: `action_field`/`date_field`/`data_term_field` must hit
   keyword/date fields (check with `GET <index>/_mapping`).

No runner code changes are needed — what varies per feed is config plus its
transform script.

## Local smoke test

With the docker-compose Elasticsearch up, seed a control alias + data index
(explicit keyword mappings on the control fields, mirroring the real index):

```bash
# control index behind the alias, with Start + End docs
curl -X PUT localhost:9200/ist.enterprise.c360.facility.eod.control.v1-r1 \
  -H 'Content-Type: application/json' -d '{
  "mappings": {"properties": {
    "header": {"properties": {"batchId": {"type": "keyword"}, "messageId": {"type": "keyword"}}},
    "control": {"properties": {"action": {"type": "keyword"},
                 "eodDate": {"type": "date", "format": "yyyy-MM-dd"},
                 "batchSizeIntended": {"type": "integer"}}}}},
  "aliases": {"ist.enterprise.c360.facility.eod.control.v1": {}}}'
curl -X POST localhost:9200/ist.enterprise.c360.facility.eod.control.v1-r1/_doc/msg-end-1 \
  -H 'Content-Type: application/json' -d '{"header":{"batchId":"C360_SMOKE","messageId":"msg-end-1"},
  "control":{"action":"End","eodDate":"2026-06-30","batchSizeIntended":3}}'

# data index behind its alias (dynamic mapping gives header.batchId.keyword)
curl -X PUT localhost:9200/ist.enterprise.c360.facility.eod.data.v1-r1 \
  -H 'Content-Type: application/json' \
  -d '{"aliases": {"ist.enterprise.c360.facility.eod.data.v1": {}}}'
for i in 1 2 3; do curl -X POST \
  localhost:9200/ist.enterprise.c360.facility.eod.data.v1-r1/_doc/fac-$i \
  -H 'Content-Type: application/json' \
  -d "{\"header\":{\"batchId\":\"C360_SMOKE\",\"messageId\":\"m-$i\"},
       \"facility\":{\"facilityId\":\"FAC-$i\",\"facilityName\":\"Facility $i\"}}"; done
curl -X POST localhost:9200/_refresh
```

Then run the pipeline twice — the first run does all three steps, the second
skips extract/convert (idempotent) while still re-polling the control index:

```bash
./scripts/run_eod.sh --env local --date 2026-06-30 --lookback-days 0
```

Clean up with:

```bash
curl -X DELETE 'localhost:9200/ist.enterprise.c360.facility.eod.*.v1-r1'
rm -rf data/state data/csv data/logs data/es_results/facility-*
```

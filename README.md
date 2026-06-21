# Kafka Codex ETL

Two independent Python 3.11 components:

1. `etl.kafka_consumer` listens to a Kafka control topic, converts each message to a canonical JSON file, and optionally triggers the Elasticsearch lookup component.
2. `etl.es_lookup` accepts a converted Kafka JSON file, a raw JSON object, a query JSON object, or a direct ID, then queries an Elasticsearch index.

Both components can run manually for debugging and both load environment-specific TOML configs.

## Install

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -e .
```

If `python3.11` is not installed locally, Python 3.12 will run this code too, but the target runtime is 3.11.

## Configs

Configs live in `config/`:

- `config/local.toml`
- `config/dev.toml`
- `config/prod.toml`

Select an environment with `--env local`, `--env dev`, or `--env prod`.

## Bootstrap Smoke Tests

These do not require Kafka or Elasticsearch.

```bash
./scripts/bootstrap_consumer.sh
./scripts/bootstrap_es_lookup.sh
```

The consumer bootstrap reads `samples/control_message.json`, writes a converted JSON file under `data/converted/`, and triggers the lookup component in dry-run mode.

## Local Docker Integration Test

Start Kafka and Elasticsearch:

```bash
docker compose up -d kafka elasticsearch
```

Run the end-to-end test:

```bash
./scripts/bootstrap_e2e_docker.sh
```

That script indexes a test Elasticsearch document, publishes `samples/control_message.json` to Kafka, consumes one Kafka batch, saves the converted JSON, and triggers the Elasticsearch lookup.

## Rebuild Tutorial

For a line-by-line developer walkthrough that starts from an empty project and rebuilds the codebase with tests at each stage, see `docs/rebuild-from-scratch-tutorial.md`.

## Run Kafka Consumer

Listen continuously:

```bash
python -m etl.kafka_consumer --env dev
```

By default, the live consumer processes at most one configured batch every 10 minutes. Tune that in the selected config with:

```toml
[consumer]
batch_max_records = 100
batch_interval_seconds = 600
poll_timeout_ms = 1000
```

Read one Kafka batch then exit:

```bash
python -m etl.kafka_consumer --env dev --once
```

Debug without Kafka:

```bash
python -m etl.kafka_consumer --env local \
  --message-file samples/control_message.json \
  --trigger-next
```

## Run Elasticsearch Lookup

From a converted Kafka message:

```bash
python -m etl.es_lookup --env dev --message-file data/converted/message.json
```

From a raw JSON object:

```bash
python -m etl.es_lookup --env dev --json '{"event_id":"customer-123"}'
```

From an Elasticsearch query:

```bash
python -m etl.es_lookup --env dev --query-json '{"term":{"event_id":"customer-123"}}'
```

From a direct ID:

```bash
python -m etl.es_lookup --env dev --id customer-123
```

Dry run without Elasticsearch:

```bash
python -m etl.es_lookup --env local --id customer-123 --dry-run
```

## Details To Fill In

Update the config files with your real:

- Kafka bootstrap servers, topic, group id, and security settings.
- Elasticsearch hosts, index name, auth, and TLS settings.
- Control-message ID field name if it is not `event_id`.

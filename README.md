# Kafka Codex ETL

Two independent Python 3.11 components:

1. `etl.kafka_consumer` listens to a Kafka control topic, **deserializes each Avro
   message through Schema Registry**, converts it to a canonical JSON file, and optionally
   triggers the Elasticsearch lookup component.
2. `etl.es_lookup` accepts a converted Kafka JSON file, a raw JSON object, a query JSON object, or a direct ID, then queries an Elasticsearch index.

Both components can run manually for debugging and both load environment-specific TOML configs.

The Kafka clients use **`confluent-kafka`** (librdkafka) with Confluent **Schema Registry /
Avro** and support Kerberos (SASL/GSSAPI) — mirroring the production reference in
[`reference/confluent_avro_consumer.py`](reference/confluent_avro_consumer.py). See
[`docs/real-world-kerberos-avro.md`](docs/real-world-kerberos-avro.md) for the design notes.

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
- `config/local-kerberos.toml` (local Kafka over SASL/GSSAPI — see [Kerberos Authentication](#kerberos-authentication-optional))

Select an environment with `--env local`, `--env dev`, `--env prod`, or `--env local-kerberos`.

## Bootstrap Smoke Tests

These do not require Kafka or Elasticsearch.

```bash
./scripts/bootstrap_consumer.sh
./scripts/bootstrap_es_lookup.sh
```

The consumer bootstrap reads `samples/control_message.json`, writes a converted JSON file under `data/converted/`, and triggers the lookup component in dry-run mode.

## Local Docker Integration Test

Start Kafka, Schema Registry, and Elasticsearch:

```bash
docker compose up -d
```

The broker exposes two listeners: `HOST` (advertised `localhost:9092`, for tools on your
machine) and `INTERNAL` (advertised `kafka:29092`, used by Schema Registry and other
containers). Schema Registry is on `localhost:8081`.

Run the end-to-end test:

```bash
./scripts/bootstrap_e2e_docker.sh
```

That script indexes a test Elasticsearch document, publishes `samples/control_message.json`
to Kafka **Avro-encoded (registering the schema under `control-topic-value`)**, consumes one
message, **deserializes it via Schema Registry**, saves the converted JSON, and triggers the
Elasticsearch lookup. The Avro value schema is `samples/control_message.avsc`.

## Kerberos Authentication (Optional)

Kafka can run with SASL/GSSAPI (Kerberos) authentication. It is **off by default**
(plain `PLAINTEXT`) and toggled entirely from a compose file selector — no edits to
`docker-compose.yml` required.

The Kerberos overlay (`docker-compose.kerberos.yml`) adds an MIT Kerberos KDC that mints
the broker and client keytabs, and switches only the `HOST` client listener (9092) to
SASL_PLAINTEXT/GSSAPI. The `INTERNAL` listener (`kafka:29092`) stays `PLAINTEXT`, so
inter-broker traffic and Schema Registry keep working without Kerberos — only external
clients authenticate.

Toggle it with the `COMPOSE_FILE` variable in `.env` (Docker Compose reads `.env`
automatically):

```bash
cp .env.example .env
# Kerberos OFF (default):  COMPOSE_FILE=docker-compose.yml
# Kerberos ON:             COMPOSE_FILE=docker-compose.yml:docker-compose.kerberos.yml
docker compose up -d
```

Or pass the files explicitly without an `.env`:

```bash
docker compose -f docker-compose.yml -f docker-compose.kerberos.yml up -d
```

Verify the broker authenticated and that GSSAPI produce/consume works end to end. The
script runs the Kafka console tools **inside** the broker container (so it does not depend
on a GSSAPI-enabled `librdkafka` on your host), logging in straight from the client keytab —
no `kinit` needed:

```bash
# broker obtained its TGT as kafka/localhost@EXAMPLE.COM
docker logs kafka-codex-kafka 2>&1 | grep -i "TGT valid"

# produce + consume one message over SASL_PLAINTEXT/GSSAPI
./scripts/verify_kerberos_broker.sh
```

The realm (`EXAMPLE.COM`), KDC, principals, krb5/JAAS config, and the KDC Docker image
live under `kerberos/`. Files:

- `kerberos/Dockerfile.kdc` + `kerberos/kdc-entrypoint.sh` — the KDC; creates the
  `kafka/localhost` and `client` principals and exports their keytabs.
- `kerberos/krb5.conf` — shared realm/KDC config.
- `kerberos/kafka_jaas.conf` — broker login (service-principal keytab).
- `kerberos/client_jaas.conf` — CLI client login (client keytab).

### Running the ETL over Kerberos

The Python components authenticate using the native librdkafka properties in the selected
config. The `local-kerberos` environment (`config/local-kerberos.toml`) sets
`security.protocol = "SASL_PLAINTEXT"`, `sasl.mechanism = "GSSAPI"`,
`sasl.kerberos.service.name`, and a keytab/principal under `[kafka.client]`:

```bash
python -m etl.kafka_consumer --env local-kerberos --once
```

> **Caveat (verified on this machine):** confluent-kafka's prebuilt wheels often bundle a
> `librdkafka` **without** GSSAPI — you will see `No provider for SASL mechanism GSSAPI` or
> `Property not available: "sasl.kerberos.keytab"`. Run the GSSAPI ETL path from a host or
> container whose `librdkafka` was built with SASL/GSSAPI. The **broker** side is fully
> verified by `scripts/verify_kerberos_broker.sh`. See
> [`docs/real-world-kerberos-avro.md`](docs/real-world-kerberos-avro.md) for the full
> explanation and the production `SASL_SSL` variant (`config/prod.toml`).

## Tutorials

- `docs/rebuild-from-scratch-tutorial.md` and `docs/test-driven-rebuild-tutorial.md` —
  line-by-line walkthroughs that rebuild the codebase with tests at each stage.
  **Note:** these describe the earlier `kafka-python` + JSON design; the Kafka clients have
  since moved to `confluent-kafka` + Avro/Schema Registry (this README and the source are
  current).
- `docs/real-world-kerberos-avro.md` — how the confluent-kafka + Avro + Kerberos pattern
  works and how it maps onto this project.

## Run Kafka Consumer

Listen continuously:

```bash
python -m etl.kafka_consumer --env dev
```

Each `poll()` waits up to `poll_timeout_seconds`; the consumer stops after `max_messages`
(0 = unlimited). Tune these in the selected config:

```toml
[consumer]
max_messages = 100
poll_timeout_seconds = 10.0
```

Drain the topic then exit (stops on poll timeout / end-of-partition):

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

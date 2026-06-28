# Real-World Kerberos + Avro Consumer — How It Changes Things

This note explains the production consumer pattern captured in
[`reference/confluent_avro_consumer.py`](../reference/confluent_avro_consumer.py)
(transcribed from the Streaming Architecture wiki), how it differs from the
`etl` package in this repo, and what you need to change to authenticate and to
decode Avro.

There are **two big differences** from the `etl.kafka_consumer` you have today:

1. **Authentication** — the broker is reached over `SASL_SSL` with the `GSSAPI`
   (Kerberos) mechanism, using a **keytab + principal**, a **krb5.conf**, and a
   **TLS truststore**. Plaintext is never on the wire.
2. **Payloads are Avro, not JSON** — values are Avro-encoded and must be decoded
   through **Schema Registry**. You cannot `json.loads()` the bytes; you need the
   writer's schema, which Schema Registry supplies.

Everything else (poll loop, offsets, group, print-as-JSON) is conceptually the
same as what we already have.

---

## 1. Library change: `kafka-python` → `confluent-kafka`

| | `etl` package (today) | Reference (production) |
|---|---|---|
| Client lib | `kafka-python` (pure Python) | `confluent-kafka` (wraps C `librdkafka`) |
| Kerberos provider | Python `gssapi` package | `librdkafka` built **with** GSSAPI (cyrus-sasl) |
| Avro / Schema Registry | not supported | first-class (`confluent_kafka.schema_registry`) |
| `poll()` returns | a dict `{TopicPartition: [records]}` (a batch) | a **single** `Message` (or `None`) |

This is the single most consequential change. The reason production uses
`confluent-kafka` is that `librdkafka` has battle-tested SASL/GSSAPI and the
official Schema Registry / Avro integration.

> **The #1 gotcha (called out in the script's STEP 1):** the default
> `confluent-kafka` wheel ships a `librdkafka` built **without** GSSAPI. The
> symptom is `No provider for SASL mechanism GSSAPI`. The fix is to rebuild from
> source against a SASL/GSSAPI-enabled `librdkafka`:
> ```bash
> pip3 install --no-binary confluent-kafka confluent-kafka[avro] --force-reinstall
> ```
> (kafka-python avoids this by using the pure-Python `gssapi` package, but it has
> no Avro/Schema-Registry story — which is why the platform standard is
> confluent-kafka.)

---

## 2. How authentication actually works (SASL_SSL + GSSAPI)

The whole handshake is configured by six client properties (STEP 5 of the script):

```python
"security.protocol": "SASL_SSL",                     # 1. TLS transport + SASL auth
"sasl.mechanism": "GSSAPI",                           # 2. Kerberos
"sasl.kerberos.service.name": "kafka",               # 3. broker SPN service part
"sasl.kerberos.principal": PRINCIPAL,                 # 4. *your* identity
"sasl.kerberos.keytab": KEYTAB,                       # 5. *your* credential (no password)
"ssl.ca.location": SSL_CA_CERT,                       # 6. truststore for the broker's TLS cert
"ssl.endpoint.identification.algorithm": "none",     #    skip hostname check (LB in front)
```

Step by step, what happens when the client connects:

1. **`security.protocol = SASL_SSL`** — two things layered: a **TLS** tunnel
   (encryption + the broker proves its identity with a cert) *and* **SASL**
   authentication inside it. (Our local lab uses `SASL_PLAINTEXT` — same auth,
   no TLS. Production always wants `SASL_SSL`.)
2. **`sasl.mechanism = GSSAPI`** — GSSAPI is the Kerberos mechanism. The client
   must present a Kerberos **service ticket** for the broker.
3. **Kerberos ticket acquisition** — `librdkafka` reads
   `os.environ["KRB5_CONFIG"]` (set in STEP 3) to find the realm + KDC, then uses
   the **keytab** to `kinit` the **principal** automatically. A keytab is an
   encrypted file holding the principal's long-term key, so **no interactive
   password** is needed — ideal for daemons. It gets a TGT, then requests a
   service ticket for the broker.
4. **The broker's Service Principal Name (SPN)** is
   `{sasl.kerberos.service.name}/{broker-host}@{REALM}`, e.g.
   `kafka/dp.uat.bns@BNS`. `service.name = kafka` is just the first half; the host
   comes from the advertised broker address. The KDC must have that exact SPN, and
   the broker must hold its keytab — that's the broker side (in our lab, the `kdc`
   container mints `kafka/localhost@EXAMPLE.COM`).
5. **`ssl.ca.location`** — a PEM truststore so the client can verify the broker's
   TLS certificate chain. Same PEM across IST/UAT/PROD here.
6. **`ssl.endpoint.identification.algorithm = none`** — disables hostname
   verification. Needed because clients connect through an **nginx load balancer**
   whose cert CN/SAN won't match the individual broker host. (Security trade-off:
   you still verify the cert is signed by your CA, but not that the hostname
   matches. Only acceptable behind a trusted LB.)

Per-environment, the only things that change are `BOOTSTRAP_SERVERS`,
`SCHEMA_REGISTRY_URL`, and which `krb5.conf` you point at
(`krb5_nonprod.conf` for IST/UAT vs `krb5_prod.conf` for PROD) — different realms
have different KDCs.

### Mapping onto this repo

The lab Kerberos setup I added mirrors the **broker side** of this:

- `docker-compose.kerberos.yml` + the `kerberos/` dir stand up a KDC, mint the
  `kafka/localhost` SPN keytab, and switch the broker's client listener to
  `SASL_PLAINTEXT/GSSAPI`.
- `config/local-kerberos.toml` configures the **client side** for `kafka-python`:
  ```toml
  security_protocol = "SASL_PLAINTEXT"   # lab: no TLS. Prod equivalent: "SASL_SSL"
  sasl_mechanism = "GSSAPI"
  sasl_kerberos_service_name = "kafka"
  sasl_kerberos_domain_name = "localhost"
  ```
  `etl.config.kafka_sasl_kwargs()` forwards those to the `kafka-python` client.

The conceptual gap between lab and prod is exactly: **`SASL_PLAINTEXT` → `SASL_SSL`**
(add a truststore) and **keytab is held by the broker container** vs **keytab is
held by you, the client**. The auth mechanism (GSSAPI against a `kafka/...` SPN)
is identical.

> Note: `kafka-python` does **not** auto-`kinit` from a keytab the way
> `librdkafka` does. With `kafka-python` you must have a valid TGT in the ambient
> credential cache first (`kinit -kt your.keytab your_principal`). `librdkafka`
> (the reference) takes `sasl.kerberos.keytab` + `sasl.kerberos.principal` and
> does the `kinit` for you.

---

## 3. The Avro change: you must decode against a schema

In the current `etl` pipeline a Kafka value is JSON: `decode_kafka_value()` just
does `json.loads(bytes)`. **That does not work for Avro.** An Avro value on Kafka
is binary, and (with Schema Registry) it is wire-framed as:

```
[ 1 magic byte = 0x00 ][ 4-byte big-endian schema ID ][ Avro-encoded body ]
```

You cannot decode the body without the **writer's schema**, and the schema is not
in the message — only its **ID** is. Schema Registry is the service that maps
ID → schema. So decoding requires a registry client plus a deserializer:

```python
sr = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL, "ssl.ca.location": SSL_CA_CERT})
deserializer = AvroDeserializer(sr)                 # auto-detect mode
...
ctx = SerializationContext(msg.topic(), MessageField.VALUE)
value = deserializer(msg.value(), ctx)              # bytes -> Python dict
```

Three schema-resolution modes (STEP 4), in order of preference:

- **Auto-detect (recommended)** — `AvroDeserializer(sr)` with no schema. It reads
  the 4-byte ID from each message header and fetches that exact schema from the
  registry. Handles schema evolution transparently. This is what consumers should
  use.
- **By subject** — `sr.get_latest_version(f"{TOPIC}-value")`. The Schema Registry
  subject convention is `<topic>-value` (and `<topic>-key`). Pins to "latest",
  which is riskier if producers evolve the schema.
- **By schema ID** — `sr.get_schema(11885)`. Pins to one exact version. Use only
  when you must.

`SerializationContext(topic, MessageField.VALUE)` tells the deserializer this is a
**value** (vs a key) for that topic — Schema Registry subjects are
per-topic-per-field, so the context selects the right subject.

After this line, `value` is a plain Python `dict`, and from there the rest looks
just like our pipeline (print as JSON / convert / save).

### Mapping onto this repo

`etl` has no Avro support today. If the upstream topic is Avro, the change is
localized to **decoding**: replace `decode_kafka_value()`'s `json.loads` with an
`AvroDeserializer` call, and add Schema-Registry settings to config
(`schema_registry_url`, truststore). The conversion step (`convert_control_message`)
is unaffected — it already takes a `dict`. The `id_attribute` lookup would then
read a field out of the Avro-derived dict instead of a JSON-derived one.

---

## 4. Consume-loop differences (smaller, but real)

| Concern | `etl.kafka_consumer` (kafka-python) | Reference (confluent-kafka) |
|---|---|---|
| Poll result | `poll()` → dict of partition→records; we `flatten_polled_records` | `poll(timeout)` → one `Message` or `None` |
| End-of-data | empty dict → keep looping | `msg is None` (timeout) → break; `KafkaError._PARTITION_EOF` → end of partition |
| Errors | exceptions | `msg.error()` is checked on every message |
| Decode | `json.loads` | `AvroDeserializer(...)` with a `SerializationContext` |
| Stop condition | `--once` / batch interval | `MAX_MESSAGES`, timeout, or Ctrl-C |
| Cleanup | `consumer.close()` in `finally` | `consumer.close()` in `finally` (commits offsets, leaves group) |

Both commit offsets via `enable.auto.commit=True` and a clean `close()`.

---

## 5. TL;DR — what "significantly changes"

1. **Switch client library** to `confluent-kafka`, and make sure its `librdkafka`
   was built **with GSSAPI** (else `No provider for SASL mechanism GSSAPI`).
2. **Authenticate with `SASL_SSL` + `GSSAPI`**: keytab + principal (librdkafka
   auto-`kinit`s), `KRB5_CONFIG` for the realm/KDC, a PEM truststore for TLS, and
   `ssl.endpoint.identification.algorithm=none` when behind a load balancer.
3. **Decode Avro through Schema Registry**: the value is `magic+schemaID+body`;
   use `SchemaRegistryClient` + `AvroDeserializer` (auto-detect by header ID) and a
   `SerializationContext(topic, MessageField.VALUE)` to get a Python dict — only
   then does it look like the JSON pipeline we already have.
4. **Per-environment config** = bootstrap servers + schema registry URL + which
   `krb5.conf` (nonprod vs prod realm). Everything else is constant.

#!/usr/bin/env python3
"""
GBMDP Kafka Avro consumer (reference implementation).

Transcribed from the "Streaming Architecture / How-to articles" wiki page.
This is the REAL-WORLD pattern the platform uses: confluent-kafka (librdkafka)
with Schema Registry / Avro and Kerberos (SASL_SSL + GSSAPI) authentication.

It is kept here as a reference/guide, not wired into the etl package. The etl
package uses kafka-python; see docs/real-world-kerberos-avro.md for how the two
differ and how to map this pattern onto the project.

Consumes Avro messages from Kafka, deserializes using Schema Registry,
and prints each record as JSON.

Usage:
    python3 consumer.py
"""

import os
import sys
import json
from datetime import datetime


# =====================================================================
# STEP 1: Dependencies
# =====================================================================
# pip3 install confluent-kafka[avro]
#
# IMPORTANT: The default pip package does NOT support Kerberos (GSSAPI).
# If you get "No provider for SASL mechanism GSSAPI", you need to:
#    1. Build librdkafka from source with GSSAPI support
#    2. Reinstall: pip3 install --no-binary confluent-kafka confluent-kafka[avro] --force-reinstall
# See GBMDP_KAFKA_PYTHON_GUIDE.md for full instructions.
# =====================================================================
from confluent_kafka import Consumer, KafkaError                              # Kafka consumer client
from confluent_kafka.serialization import SerializationContext, MessageField  # Avro deserialization context
from confluent_kafka.schema_registry import SchemaRegistryClient              # Schema Registry client
from confluent_kafka.schema_registry.avro import AvroDeserializer             # Avro deserializer


# =====================================================================
# STEP 2: Configuration
# =====================================================================
# Switch environment by uncommenting the appropriate block below.
# Each environment has its own bootstrap server, schema registry, keytab, and principal.
# =====================================================================

# --- IST ---
# BOOTSTRAP_SERVERS = "dp.ist.bns:9040"
# SCHEMA_REGISTRY_URL = "https://dp.ist.bns:1443"

# --- UAT ---
BOOTSTRAP_SERVERS = "dp.uat.bns:9040"
SCHEMA_REGISTRY_URL = "https://dp.uat.bns:1443"

# --- PROD ---
# BOOTSTRAP_SERVERS = "dp.bns:9040"
# SCHEMA_REGISTRY_URL = "https://dp.bns:1443"

# Kerberos - replace with your principal and keytab
PRINCIPAL = "your_principal@BNS"
KEYTAB = "/path/to/your_principal.keytab"
KRB5_CONF = "/path/to/krb5.conf"  # krb5_nonprod.conf for IST/UAT, krb5_prod.conf for PROD

# SSL - same PEM truststore for all environments
SSL_CA_CERT = "/path/to/complete_truststore.pem"

# Topic and Schema
TOPIC = "uat3.master.pxv.trade.eod.producer"

# Option A: Auto-detect schema from message header (recommended for consumers)
#    Each Avro message contains the schema ID in its header - the deserializer
#    fetches the correct schema automatically. No config needed.
SCHEMA_SUBJECT = None

# Option B: Use subject name to get the latest schema
#    Subject name follows the convention: <topic-name>-value
# SCHEMA_SUBJECT = f"{TOPIC}-value"

# Option C: Use a specific schema ID
#    Useful when you need to pin to a specific schema version.
# SCHEMA_SUBJECT = None
# SCHEMA_ID = 11885
SCHEMA_ID = None  # set by Option C above, leave as None for Option A/B

# Consumer settings
CONSUMER_GROUP = "my-consumer-group"  # unique per application - change this
MAX_MESSAGES = 100                    # number of messages to consume then exit (None = unlimited)
FROM_BEGINNING = True                 # True = read from earliest, False = read from latest
POLL_TIMEOUT = 10.0                   # seconds to wait before assuming no more messages


# =====================================================================
# STEP 3: Kerberos setup
# =====================================================================
# librdkafka uses this env var to locate the KDC configuration.
# It will automatically call kinit using the keytab and principal above.
# =====================================================================
os.environ["KRB5_CONFIG"] = KRB5_CONF


# =====================================================================
# STEP 4: Connect to Schema Registry and create Avro deserializer
# =====================================================================
# The deserializer converts Avro bytes back to Python dicts.
# Three modes:
#    - Auto-detect: reads schema ID from each message header (default, most flexible)
#    - Subject name: fetches latest schema for the topic's subject
#    - Schema ID: pins to a specific schema version
# =====================================================================
print(f"Connecting to Schema Registry: {SCHEMA_REGISTRY_URL}")

sr = SchemaRegistryClient({
    "url": SCHEMA_REGISTRY_URL,
    "ssl.ca.location": SSL_CA_CERT,
})

if SCHEMA_SUBJECT:
    # Fetch latest schema by subject name
    schema_version = sr.get_latest_version(SCHEMA_SUBJECT)
    deserializer = AvroDeserializer(sr, schema_version.schema.schema_str)
    print(f"Using schema subject={SCHEMA_SUBJECT}, id={schema_version.schema_id}, version={schema_version.version}")
elif SCHEMA_ID:
    # Fetch schema by specific ID
    schema = sr.get_schema(SCHEMA_ID)
    deserializer = AvroDeserializer(sr, schema.schema_str)
    print(f"Using schema ID {SCHEMA_ID}")
else:
    # Auto-detect from message header (recommended for consumers)
    deserializer = AvroDeserializer(sr)
    print("Using schema from message header (auto-detect)")


# =====================================================================
# STEP 5: Create Kafka Consumer
# =====================================================================
# security.protocol = SASL_SSL                  -> encrypted connection + SASL authentication
# sasl.mechanism = GSSAPI                       -> Kerberos authentication
# sasl.kerberos.service.name = kafka            -> Kafka broker's Kerberos service name
# ssl.endpoint.identification.algorithm = none  -> skip hostname verification (nginx LB)
# group.id                                      -> consumer group - messages are load-balanced within a group
# auto.offset.reset = earliest/latest           -> where to start if no committed offset exists
# enable.auto.commit = True                     -> auto-commit offsets periodically
# =====================================================================
print(f"Connecting to Kafka: {BOOTSTRAP_SERVERS}")
print(f"Topic: {TOPIC}, Group: {CONSUMER_GROUP}")

consumer = Consumer({
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "GSSAPI",
    "sasl.kerberos.service.name": "kafka",
    "sasl.kerberos.principal": PRINCIPAL,
    "sasl.kerberos.keytab": KEYTAB,
    "ssl.ca.location": SSL_CA_CERT,
    "ssl.endpoint.identification.algorithm": "none",
    "group.id": CONSUMER_GROUP,
    "auto.offset.reset": "earliest" if FROM_BEGINNING else "latest",
    "enable.auto.commit": True,
})

consumer.subscribe([TOPIC])
print(f"Subscribed - waiting for messages...")


# =====================================================================
# STEP 6: Consume messages
# =====================================================================
# The consume loop:
#    1. Poll for next message (waits up to POLL_TIMEOUT seconds)
#    2. Deserialize Avro bytes -> Python dict using Schema Registry
#    3. Print the record as JSON
#    4. Stop after MAX_MESSAGES or on timeout / Ctrl+C
# =====================================================================
consumed = 0
errors = 0

try:
    while True:
        msg = consumer.poll(timeout=POLL_TIMEOUT)

        # No message within timeout
        if msg is None:
            print("No more messages - timeout reached")
            break

        # Kafka error (e.g., partition EOF)
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print(f"End of partition {msg.partition()}")
                if MAX_MESSAGES:
                    break
                continue
            else:
                print(f"ERROR: {msg.error()}")
                errors += 1
                continue

        # Deserialize Avro bytes -> Python dict
        try:
            ctx = SerializationContext(msg.topic(), MessageField.VALUE)
            value = deserializer(msg.value(), ctx)
        except Exception as e:
            errors += 1
            print(f"ERROR: Deserialization failed (offset={msg.offset()}): {e}")
            continue

        consumed += 1

        # Print the message as JSON
        print(json.dumps({
            "partition": msg.partition(),
            "offset": msg.offset(),
            "key": msg.key().decode("utf-8") if msg.key() else None,
            "value": value,
        }, default=str, indent=2))

        # Stop after N messages
        if MAX_MESSAGES and consumed >= MAX_MESSAGES:
            print(f"Reached {MAX_MESSAGES} messages - stopping")
            break

except KeyboardInterrupt:
    print("\nInterrupted by user")

finally:
    # Always close consumer to commit offsets and leave consumer group cleanly
    consumer.close()


# =====================================================================
# STEP 7: Summary
# =====================================================================
print("=" * 60)
print(f"COMPLETE")
print(f"  Consumed: {consumed:,}")
print(f"  Errors:   {errors:,}")
print("=" * 60)

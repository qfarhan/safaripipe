#!/usr/bin/env bash
# Prove the broker accepts Kerberos (SASL_PLAINTEXT/GSSAPI) auth, end to end,
# using the Kafka console tools INSIDE the broker container. This sidesteps the
# host's librdkafka (whose prebuilt wheel often lacks GSSAPI) and the
# advertised-listener addressing, since inside the container localhost:9092 is
# the broker's SASL listener.
#
# Prereqs: the Kerberos stack is up, e.g.
#   docker compose -f docker-compose.yml -f docker-compose.kerberos.yml up -d
#
# The client logs in straight from client.keytab (Krb5LoginModule useKeyTab), so
# no `kinit` is needed.
set -euo pipefail

KAFKA_CONTAINER="${KAFKA_CONTAINER:-kafka-codex-kafka}"
TOPIC="${TOPIC:-krb-smoke}"
MESSAGE="${MESSAGE:-hello-kerberos}"

# Point the JVM at the CLIENT JAAS (KafkaClient entry) + the realm config.
CLIENT_OPTS="-Djava.security.auth.login.config=/etc/kafka/kerberos/client_jaas.conf -Djava.security.krb5.conf=/etc/kafka/kerberos/krb5.conf"

SASL_PROPS=(
  --producer-property security.protocol=SASL_PLAINTEXT
  --producer-property sasl.mechanism=GSSAPI
  --producer-property sasl.kerberos.service.name=kafka
)
SASL_CONS_PROPS=(
  --consumer-property security.protocol=SASL_PLAINTEXT
  --consumer-property sasl.mechanism=GSSAPI
  --consumer-property sasl.kerberos.service.name=kafka
)

echo "[verify] producing '${MESSAGE}' to '${TOPIC}' over SASL_PLAINTEXT/GSSAPI..."
docker exec -i -e KAFKA_OPTS="${CLIENT_OPTS}" "${KAFKA_CONTAINER}" \
  bash -c "echo '${MESSAGE}' | /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server localhost:9092 --topic '${TOPIC}' ${SASL_PROPS[*]}"

echo "[verify] consuming it back over SASL_PLAINTEXT/GSSAPI..."
docker exec -e KAFKA_OPTS="${CLIENT_OPTS}" "${KAFKA_CONTAINER}" \
  /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 --topic "${TOPIC}" \
    --from-beginning --timeout-ms 15000 --max-messages 1 "${SASL_CONS_PROPS[@]}"

echo "[verify] OK: broker authenticated the GSSAPI client and round-tripped a message."

#!/usr/bin/env bash
# Prove Kerberos (SASL_PLAINTEXT/GSSAPI, keytab) and Avro/Schema-Registry decode
# work TOGETHER in one etl.kafka_consumer run, from the HOST (i.e. through the
# host's librdkafka — unlike verify_kerberos_broker.sh, which only proves the
# broker side using the Java console tools inside the container).
#
# What it does:
#   1. copies the client keytab out of the KDC container to kerberos/keytabs/
#   2. points Kerberos at the host-side realm config (kerberos/krb5-host.conf)
#   3. produces the Start + End sample messages (Avro via Schema Registry, over GSSAPI)
#   4. runs the consumer once and asserts the save-gate skipped Start and saved End
#
# Prereqs: the Kerberos stack is up, e.g.
#   docker compose -f docker-compose.yml -f docker-compose.kerberos.yml up -d
#
# Requires a librdkafka built WITH GSSAPI; if you see
# 'No provider for SASL mechanism GSSAPI', see docs/real-world-kerberos-avro.md.
set -euo pipefail

cd "$(dirname "$0")/.."

KDC_CONTAINER="${KDC_CONTAINER:-kafka-codex-kdc}"
PYTHON="${PYTHON:-.venv/bin/python}"
[ -x "${PYTHON}" ] || PYTHON="python3"

if ! docker ps --format '{{.Names}}' | grep -qx "${KDC_CONTAINER}"; then
  echo "[verify] ERROR: KDC container '${KDC_CONTAINER}' is not running." >&2
  echo "         Start it with: docker compose -f docker-compose.yml -f docker-compose.kerberos.yml up -d" >&2
  exit 1
fi

echo "[verify] exporting client keytab from ${KDC_CONTAINER}..."
mkdir -p kerberos/keytabs
docker cp -q "${KDC_CONTAINER}:/keytabs/client.keytab" kerberos/keytabs/client.keytab

# Host-side Kerberos env: realm config that reaches the KDC via localhost:88,
# and a private credential cache so we never touch the user's default one.
export KRB5_CONFIG="${PWD}/kerberos/krb5-host.conf"
export KRB5CCNAME="FILE:$(mktemp -t krb5cc_kafka_codex_verify)"
trap 'rm -f "${KRB5CCNAME#FILE:}"' EXIT
export PYTHONPATH="${PYTHONPATH:-}:src"

echo "[verify] producing Start (must be skipped) + End (must be saved) over GSSAPI+Avro..."
"${PYTHON}" -m etl.kafka_producer --env local-kerberos \
  --message-file samples/control_message_start.json --key verify-start >/dev/null
"${PYTHON}" -m etl.kafka_producer --env local-kerberos \
  --message-file samples/control_message.json --key verify-end >/dev/null

echo "[verify] consuming over GSSAPI, decoding via Schema Registry..."
OUTPUT="$("${PYTHON}" -m etl.kafka_consumer --env local-kerberos --once --no-trigger-next)"
echo "${OUTPUT}"

# The consumer group's committed offsets persist, so a rerun only sees the two
# messages produced above: exactly one skip (Start) and one save (End).
SKIPPED=$(grep -c '"skipped": true' <<<"${OUTPUT}" || true)
SAVED=$(grep -c '"skipped": false' <<<"${OUTPUT}" || true)
if [ "${SKIPPED}" -lt 1 ] || [ "${SAVED}" -lt 1 ]; then
  echo "[verify] FAIL: expected >=1 skipped and >=1 saved message (got skipped=${SKIPPED}, saved=${SAVED})." >&2
  exit 1
fi

echo "[verify] OK: Kerberos keytab auth + Avro/Schema-Registry decode + EOD save-gate all worked in one consume flow (skipped=${SKIPPED}, saved=${SAVED})."

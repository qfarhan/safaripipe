#!/usr/bin/env bash
# Bootstrap an MIT Kerberos KDC and export the keytabs Kafka + clients need.
# Idempotent: the KDC database lives on a volume, so restarts reuse it.
set -euo pipefail

REALM="${KRB5_REALM:-EXAMPLE.COM}"
KDC_PASSWORD="${KRB5_KDC_PASSWORD:-masterkey}"

# Principals to create and the keytabs to export them into.
# Format: "<principal-without-realm>:<keytab-path>"
PRINCIPALS=(
  "kafka/localhost:/keytabs/kafka.keytab"   # broker service principal (SPN)
  "client:/keytabs/client.keytab"           # an example client identity
)

mkdir -p /etc/krb5kdc /keytabs /var/lib/krb5kdc

# Minimal kdc.conf + ACL. krb5.conf itself is mounted at /etc/krb5.conf.
cat > /etc/krb5kdc/kdc.conf <<EOF
[kdcdefaults]
    kdc_ports = 88
    kdc_tcp_ports = 88

[realms]
    ${REALM} = {
        acl_file = /etc/krb5kdc/kadm5.acl
        # Keep the master-key stash on the persistent volume next to the DB.
        # Debian's default is under /etc/krb5kdc, which does NOT survive a
        # container recreate, leaving a DB whose master key can't be fetched.
        key_stash_file = /var/lib/krb5kdc/.k5.${REALM}
        max_renewable_life = 7d 0h 0m 0s
        supported_enctypes = aes256-cts-hmac-sha1-96:normal aes128-cts-hmac-sha1-96:normal
        default_principal_flags = +preauth
    }
EOF
echo "*/admin@${REALM} *" > /etc/krb5kdc/kadm5.acl

# Create the database once; reuse it on later boots. DB_FRESH drives whether we
# must overwrite keytabs: a freshly created DB has new keys, so any keytab left
# over on the shared volume from a previous DB is stale and must be re-exported.
DB_FRESH=0
if [ ! -f /var/lib/krb5kdc/principal ]; then
    echo "[kdc] creating realm ${REALM}"
    kdb5_util create -s -r "${REALM}" -P "${KDC_PASSWORD}"
    DB_FRESH=1
else
    echo "[kdc] reusing existing database for realm ${REALM}"
fi

# Ensure each principal exists, then export its keytab when missing or stale.
# Note: kadmin.local exits 0 even when a query fails, so we test existence by
# matching the principal against `listprincs` output rather than trusting $?.
# -norandkey extracts the principal's *current* key (no rotation), so exporting
# is deterministic and the broker's keytab always matches the live DB.
for entry in "${PRINCIPALS[@]}"; do
    principal="${entry%%:*}"
    keytab="${entry##*:}"
    if kadmin.local -q "listprincs" 2>/dev/null | grep -qx "${principal}@${REALM}"; then
        echo "[kdc] principal ${principal}@${REALM} already exists"
    else
        echo "[kdc] adding principal ${principal}@${REALM}"
        kadmin.local -q "addprinc -randkey ${principal}"
    fi
    if [ "${DB_FRESH}" = "1" ] || [ ! -f "${keytab}" ]; then
        echo "[kdc] exporting keytab ${keytab} for ${principal}"
        rm -f "${keytab}"
        kadmin.local -q "ktadd -k ${keytab} -norandkey ${principal}"
    fi
    if [ ! -f "${keytab}" ]; then
        echo "[kdc] ERROR: keytab ${keytab} was not created" >&2
        exit 1
    fi
    # World-readable so the (non-root) Kafka container user can read them.
    chmod 644 "${keytab}"
done

echo "[kdc] ready; starting krb5kdc in foreground"
# -n keeps krb5kdc in the foreground so it stays PID 1's child and logs to stdout.
exec krb5kdc -n

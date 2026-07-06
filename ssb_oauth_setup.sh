#!/usr/bin/env bash
set -euo pipefail

if [ $# -lt 1 ] || [ $# -gt 2 ]; then
  echo "Usage: $0 <console-url> [kafka-bootstrap-url]"
  echo ""
  echo "With 1 arg:  prints OAuth info needed for SSB/Flink deployment (JWKS URL, base64 cert, issuer)"
  echo "With 2 args: downloads certs, creates credentials, writes test_flink.py for SSB"
  echo ""
  echo "Example: $0 https://console.rs07-ds2goeas.awcdev2.sandbox21.cloudera.com/"
  echo "Example: $0 https://console.rs07-ds2goeas.awcdev2.sandbox21.cloudera.com/ rs07-kafka-kafka.rs07-kafka.awcdev2.sandbox21.cloudera.com:8443"
  exit 1
fi

CONSOLE_URL="$1"

# Directory where output files will be saved
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Extract console host and domain
CONSOLE_HOST=$(echo "$CONSOLE_URL" | sed -E 's|https?://([^/]+).*|\1|')
[[ "$CONSOLE_HOST" == *:* ]] || CONSOLE_HOST="${CONSOLE_HOST}:443"
DOMAIN=$(echo "$CONSOLE_HOST" | sed -E 's/^console\.//; s/:[0-9]+$//')

# --- Info-only mode (1 argument) ---
if [ $# -eq 1 ]; then
  echo "=== OAuth Info for ${DOMAIN} ==="
  echo ""
  echo "JWKS URL: https://knox.${DOMAIN}/gateway/knox-tokenexchange/knoxtoken/api/v1/jwks.json"
  echo ""
  echo "CONSOLE PEM BASE64 ENCODED:"
  openssl s_client -showcerts -connect "$CONSOLE_HOST" </dev/null 2>/dev/null \
    | openssl x509 -outform PEM > "${SCRIPT_DIR}/console.pem"
  base64 < "${SCRIPT_DIR}/console.pem"
  echo ""
  echo "ISSUER: http://knox.knox.svc.cluster.local:8443/gateway/knox-tokenexchange"
  exit 0
fi

# --- Full setup mode (2 arguments) ---
KAFKA_URL="$2"

AWC_USERNAME="admin"
AWC_PASSWORD="awc-admin-password"

KAFKA_HOST=$(echo "$KAFKA_URL" | sed -E 's|https?://([^/]+).*|\1|')
[[ "$KAFKA_HOST" == *:* ]] || KAFKA_HOST="${KAFKA_HOST}:443"

PEM_DIR="${SCRIPT_DIR}/pem_files"
mkdir -p "${PEM_DIR}"

echo "=== Downloading certificates ==="

echo "Fetching Kafka cert from ${KAFKA_HOST}..."
openssl s_client -showcerts -connect "$KAFKA_HOST" </dev/null 2>/dev/null | openssl x509 -outform PEM > "${PEM_DIR}/kafka.pem"
echo "Saved to ${PEM_DIR}/kafka.pem"

echo "Fetching Console cert from ${CONSOLE_HOST}..."
openssl s_client -showcerts -connect "$CONSOLE_HOST" </dev/null 2>/dev/null | openssl x509 -outform PEM > "${PEM_DIR}/console.pem"
echo "Saved to ${PEM_DIR}/console.pem"

# Read PEM content as single-line strings for inline embedding in Flink SQL
KAFKA_PEM_INLINE=$(tr '\n' ' ' < "${PEM_DIR}/kafka.pem" | sed 's/ $//')
CONSOLE_PEM_INLINE=$(tr '\n' ' ' < "${PEM_DIR}/console.pem" | sed 's/ $//')

echo ""
echo "=== Obtaining hadoop-jwt ==="

REDIRECT_URL=$(curl -sk "https://console.${DOMAIN}/" -o /dev/null -w '%{redirect_url}')
curl -sk -u "${AWC_USERNAME}:${AWC_PASSWORD}" -c "${SCRIPT_DIR}/cookies.txt" "${REDIRECT_URL}" -o /dev/null

HADOOP_JWT=$(grep hadoop-jwt "${SCRIPT_DIR}/cookies.txt" 2>/dev/null | awk '{print $NF}' || true)

if [ -z "$HADOOP_JWT" ]; then
  echo "Could not obtain hadoop-jwt automatically. Environment may use SSO/SAML."
  echo "Please obtain the hadoop-jwt from your browser and paste it below."
  echo ""
  read -rp "hadoop-jwt: " HADOOP_JWT
  if [ -z "$HADOOP_JWT" ]; then
    echo "ERROR: No hadoop-jwt provided."
    exit 1
  fi
else
  echo "JWT obtained"
fi

echo ""
echo "=== Obtaining access key credentials ==="

CREDS_JSON=$(curl -sk \
  -H "Cookie: hadoop-jwt=${HADOOP_JWT}" \
  -H "Content-Type: application/json" \
  -X POST "https://console.${DOMAIN}/api/v0/auth/access-keys/credentials" \
  -d '{"description": "my automation keys"}')

CLIENT_ID=$(echo "$CREDS_JSON" | grep -o '"client_id":"[^"]*"' | cut -d'"' -f4)
CLIENT_SECRET=$(echo "$CREDS_JSON" | grep -o '"client_secret":"[^"]*"' | cut -d'"' -f4)

if [ -z "$CLIENT_ID" ] || [ -z "$CLIENT_SECRET" ]; then
  echo "ERROR: Failed to extract credentials from response:"
  echo "$CREDS_JSON"
  exit 1
fi

# Kafka bootstrap is host:port without the scheme
KAFKA_BOOTSTRAP=$(echo "$KAFKA_URL" | sed -E 's|https?://||; s|/+$||')

TOKEN_URL="https://console.${DOMAIN}/api/v0/auth/access-keys/token"

echo ""
echo "=== Generating SSB Flink SQL ==="

# Derive a table name from the domain (replace dots/hyphens with underscores)
TABLE_NAME=$(echo "${DOMAIN}" | tr '.-' '_')

cat > "${SCRIPT_DIR}/test_flink.py" <<EOF
CREATE TABLE sensor_data (
  \`sensor_id\` BIGINT,
  \`car_count\` BIGINT,
  \`timestamp\` VARCHAR(2147483647),
  \`event_time\` AS TO_TIMESTAMP(\`timestamp\`, 'yyyy-MM-dd''T''HH:mm:ss.SSSX'),
  WATERMARK FOR \`event_time\` AS \`event_time\` - INTERVAL '3' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'sensor_data',
  'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP}',
  'properties.security.protocol' = 'SASL_SSL',
  'properties.sasl.mechanism' = 'OAUTHBEARER',
  'properties.sasl.oauthbearer.token.endpoint.url' = '${TOKEN_URL}',
  'properties.ssl.truststore.type' = 'PEM',
  'properties.ssl.truststore.certificates' = '${KAFKA_PEM_INLINE}',
  'properties.sasl.login.callback.handler.class' = 'org.apache.flink.kafka.shaded.org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler',
  'properties.sasl.jaas.config' = 'org.apache.flink.kafka.shaded.org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required sasl.oauthbearer.token.endpoint.url="${TOKEN_URL}" ssl.truststore.type="PEM" clientId="${CLIENT_ID}" clientSecret="${CLIENT_SECRET}" ssl.truststore.certificates="${CONSOLE_PEM_INLINE}";',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);
EOF

echo "Written to ${SCRIPT_DIR}/test_flink.py"

echo ""
echo "=== Done ==="
echo ""
echo "Generated files:"
echo "  ${PEM_DIR}/kafka.pem        — Kafka broker TLS certificate"
echo "  ${PEM_DIR}/console.pem     — AWC Console TLS certificate"
echo "  ${SCRIPT_DIR}/test_flink.py — Flink SQL DDL ready to paste into SSB"
echo ""
echo "Next step: Open SSB, create a new SQL job, and paste the contents of test_flink.py"

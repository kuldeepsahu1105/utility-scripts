#!/usr/bin/env bash
set -euo pipefail

if [ $# -lt 1 ] || [ $# -gt 2 ]; then
  echo "Usage: $0 <console-url> [kafka-bootstrap-url]"
  echo ""
  echo "With 1 arg:  prints OAuth info needed for secure Kafka deployment (JWKS URL, base64 cert, issuer)"
  echo "With 2 args: downloads certs, creates credentials, writes kafka.properties"
  echo ""
  echo "Example: $0 https://console.344-devcsm.awcqe2.sandbox21.cloudera.com/"
  echo "Example: $0 https://console.344-devcsm.awcqe2.sandbox21.cloudera.com/ https://oauth-kafka.oauth.awcqe2.sandbox21.cloudera.com:8443"
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
  echo "Authentication On External Access --- MUST BE ENABLED "
  echo "-----------------------------------------------"
  echo "External Access Enabled (enabled by default) --- MUST BE ENABLED "
  echo "-----------------------------------------------"
  echo "OAUTH2 JWKS Endpoint URI: https://knox.${DOMAIN}/gateway/knox-tokenexchange/knoxtoken/api/v1/jwks.json"
  echo "-----------------------------------------------"
  echo "OAUTH2 Valid Issuer URI : http://knox.knox.svc.cluster.local:8443/gateway/knox-tokenexchange"
  echo "-----------------------------------------------"
  echo ""
  echo "OAUTH2 CA Certificate Base64 encoded ENCODED:"
  openssl s_client -showcerts -connect "$CONSOLE_HOST" </dev/null 2>/dev/null \
    | openssl x509 -outform PEM > "${SCRIPT_DIR}/console.pem"
  base64 < "${SCRIPT_DIR}/console.pem" | tr -d '\n'
  echo ""
  exit 0
fi

# --- Full setup mode (2 arguments) ---
KAFKA_URL="$2"

AWC_USERNAME="admin"
AWC_PASSWORD="awc-admin-password"

KAFKA_HOST=$(echo "$KAFKA_URL" | sed -E 's|https?://([^/]+).*|\1|')
[[ "$KAFKA_HOST" == *:* ]] || KAFKA_HOST="${KAFKA_HOST}:443"

echo "=== Downloading certificates ==="

echo "Fetching Kafka cert from ${KAFKA_HOST}..."
openssl s_client -showcerts -connect "$KAFKA_HOST" </dev/null 2>/dev/null | openssl x509 -outform PEM > "${SCRIPT_DIR}/kafka.pem"
echo "Saved to ${SCRIPT_DIR}/kafka.pem"

echo "Fetching Console cert from ${CONSOLE_HOST}..."
openssl s_client -showcerts -connect "$CONSOLE_HOST" </dev/null 2>/dev/null | openssl x509 -outform PEM > "${SCRIPT_DIR}/console.pem"
echo "Saved to ${SCRIPT_DIR}/console.pem"

# Read PEM content as single-line strings
KAFKA_PEM_INLINE=$(tr '\n' ' ' < "${SCRIPT_DIR}/kafka.pem" | sed 's/ $//')
CONSOLE_PEM_INLINE=$(tr '\n' ' ' < "${SCRIPT_DIR}/console.pem" | sed 's/ $//')

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
KAFKA_DOMAIN=$(echo "$KAFKA_BOOTSTRAP" | sed 's/:[0-9]*$//')

TOKEN_URL="https://console.${DOMAIN}/api/v0/auth/access-keys/token"
JWKS_URL="https://knox.${DOMAIN}/gateway/knox-tokenexchange/knoxtoken/api/v1/jwks.json"

cat > "${SCRIPT_DIR}/kafka.properties" <<EOF
bootstrap.servers=${KAFKA_BOOTSTRAP}
security.protocol=SASL_SSL
sasl.mechanism=OAUTHBEARER
sasl.login.callback.handler.class=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler
sasl.oauthbearer.token.endpoint.url=${TOKEN_URL}
sasl.oauthbearer.client.credentials.client.id=${CLIENT_ID}
sasl.oauthbearer.client.credentials.client.secret=${CLIENT_SECRET}
sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required ssl.truststore.certificates="${CONSOLE_PEM_INLINE}" ssl.truststore.type="PEM";
ssl.truststore.certificates=${KAFKA_PEM_INLINE}
ssl.truststore.type=PEM
EOF

echo "Properties written to ${SCRIPT_DIR}/kafka.properties"

echo ""
echo "=== Done ==="
echo ""
echo "Next steps:"
echo ""
echo "1. Set environment variables (run these yourself):"
echo "   export DOMAIN=${KAFKA_DOMAIN}"
echo "   export KAFKA_BOOTSTRAP_SERVER=${KAFKA_BOOTSTRAP}"
echo "   export KAFKA_OPTS=\"-Dorg.apache.kafka.sasl.oauthbearer.allowed.urls=${TOKEN_URL}\""
echo ""
echo "2. List topics:"
echo "   kafka-topics.sh --bootstrap-server ${KAFKA_BOOTSTRAP} --command-config ${SCRIPT_DIR}/kafka.properties --list"
echo ""
echo "3. Consume from a topic:"
echo "   kafka-console-consumer.sh --bootstrap-server ${KAFKA_BOOTSTRAP} --command-config ${SCRIPT_DIR}/kafka.properties --topic <topic-name> --from-beginning"
echo ""
echo "4. Produce to a topic:"
echo "   kafka-console-producer.sh --bootstrap-server ${KAFKA_BOOTSTRAP} --producer.config ${SCRIPT_DIR}/kafka.properties --topic <topic-name>"

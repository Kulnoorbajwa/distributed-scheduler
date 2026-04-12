#!/bin/bash
# Generate self-signed TLS certificates for local development.
# These are NOT suitable for production use.
set -euo pipefail

CERT_DIR="${1:-./certs}"
mkdir -p "$CERT_DIR"

echo "Generating self-signed TLS certificates in $CERT_DIR..."

openssl req -x509 -newkey rsa:4096 \
    -keyout "$CERT_DIR/server.key" \
    -out "$CERT_DIR/server.crt" \
    -days 365 \
    -nodes \
    -subj "/CN=localhost" \
    -addext "subjectAltName=DNS:localhost,DNS:scheduler,DNS:worker-1,DNS:worker-2,DNS:worker-3,IP:127.0.0.1"

chmod 600 "$CERT_DIR/server.key"
chmod 644 "$CERT_DIR/server.crt"

echo "Done. Files:"
echo "  Certificate: $CERT_DIR/server.crt"
echo "  Private key: $CERT_DIR/server.key"

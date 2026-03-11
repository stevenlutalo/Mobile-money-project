#!/bin/bash
#
# gen_certs.sh — Generate TLS certificates for a node.
#
# Usage:
#   bash certs/gen_certs.sh node-1
#
# Creates:
#   certs/ca.key / certs/ca.pem  (Certificate Authority — shared by all nodes)
#   certs/node-1/key.pem        (Private key for node-1)
#   certs/node-1/cert.pem       (Certificate for node-1)

set -e

NODE_ID="${1:-node-1}"
CERT_DIR="certs"
NODE_CERT_DIR="$CERT_DIR/$NODE_ID"

echo "Generating certificates for $NODE_ID..."

# Create directory structure if needed
mkdir -p "$NODE_CERT_DIR"

# ────────────────────────────────────────────────────────────────
# Step 1: Create Certificate Authority (CA)
# ────────────────────────────────────────────────────────────────

# Generate a 4096-bit RSA private key for the Certificate Authority.
# The CA is the "trusted authority" that signs all node certificates.
if [ ! -f "$CERT_DIR/ca.key" ]; then
    echo "[CA] Generating CA private key..."
    openssl genrsa -out "$CERT_DIR/ca.key" 4096 2>/dev/null
fi

# Generate a self-signed CA certificate.
# Valid for 10 years (365*10 days).
# commonName="mmoney-ca" identifies this CA.
if [ ! -f "$CERT_DIR/ca.pem" ]; then
    echo "[CA] Generating CA certificate (self-signed)..."
    openssl req -new -x509 -days 3650 -key "$CERT_DIR/ca.key" -out "$CERT_DIR/ca.pem" \
        -subj "/CN=mmoney-ca" 2>/dev/null
fi

# ────────────────────────────────────────────────────────────────
# Step 2: Create Node Certificate Signing Request (CSR)
# ────────────────────────────────────────────────────────────────

# Generate a 4096-bit RSA private key for this node.
# This key is used to prove the node's identity (never shared).
echo "[Node] Generating node private key..."
openssl genrsa -out "$NODE_CERT_DIR/key.pem" 4096 2>/dev/null

# Generate a Certificate Signing Request.
# This says "Here is my public key, please sign it with your CA."
echo "[Node] Generating certificate signing request..."
openssl req -new -key "$NODE_CERT_DIR/key.pem" -out "$NODE_CERT_DIR/$NODE_ID.csr" \
    -subj "/CN=$NODE_ID/O=mmoney" 2>/dev/null

# ────────────────────────────────────────────────────────────────
# Step 3: Have the CA Sign the Node's Certificate
# ────────────────────────────────────────────────────────────────

# The CA signs the node's CSR with its private key.
# The result is a certificate that proves the node's identity.
# Valid for 1 year (365 days).
echo "[CA] Signing node certificate..."
openssl x509 -req -in "$NODE_CERT_DIR/$NODE_ID.csr" \
    -CA "$CERT_DIR/ca.pem" -CAkey "$CERT_DIR/ca.key" -CAcreateserial \
    -out "$NODE_CERT_DIR/cert.pem" -days 365 2>/dev/null

# Clean up the CSR (no longer needed)
rm "$NODE_CERT_DIR/$NODE_ID.csr"

echo "✓ Certificates generated:"
echo "  CA:   $CERT_DIR/ca.pem (shared by all nodes)"
echo "  Key:  $NODE_CERT_DIR/key.pem (private, never share)"
echo "  Cert: $NODE_CERT_DIR/cert.pem (public)"

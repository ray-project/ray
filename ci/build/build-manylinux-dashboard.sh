#!/bin/bash
set -euxo pipefail

# -----------------------------
# Install Node.js 14 in $HOME
# -----------------------------
NODE_VERSION_FULL="${NODE_VERSION_FULL:-14.21.3}"  # override via env if needed

ARCH="$(uname -m)"
case "$ARCH" in
  x86_64|amd64)
    NODE_ARCH="x64"
    NODE_SHASUM256="05c08a107c50572ab39ce9e8663a2a2d696b5d262d5bd6f98d84b997ce932d9a"
    ;;
  aarch64|arm64)
    NODE_ARCH="arm64"
    NODE_SHASUM256="f06642bfcf0b8cc50231624629bec58b183954641b638e38ed6f94cd39e8a6ef"
    ;;
  *)
    echo "Unsupported arch: $ARCH" >&2
    exit 1
    ;;
esac

NODE_BASE_URL="https://nodejs.org/dist/v${NODE_VERSION_FULL}"
NODE_TARBALL="node-v${NODE_VERSION_FULL}-linux-${NODE_ARCH}.tar.xz"
NODE_DIR="/usr/local/nodejs"

sudo mkdir -p "$NODE_DIR"

# Download tarball
curl -fsSL "${NODE_BASE_URL}/${NODE_TARBALL}" -o "$HOME/${NODE_TARBALL}"

# Verify checksum for our tarball only
echo "$NODE_SHASUM256  $HOME/${NODE_TARBALL}" | sha256sum -c -

# Extract
sudo tar -xJf "$HOME/$NODE_TARBALL" -C "$NODE_DIR"
rm -f "$HOME/$NODE_TARBALL"

# Add Node to PATH for this session
export PATH="$NODE_DIR/node-v${NODE_VERSION_FULL}-linux-${NODE_ARCH}/bin:$PATH"

# Quick sanity check
node -v
npm -v

# -----------------------------
# Build the dashboard
# -----------------------------
cd "$(dirname "$0")/../../python/ray/dashboard/client"

# Install and build
npm ci
npm run build

# Archive the output to be used in the wheel build
tar -czf dashboard_build.tar.gz -C build .
mv dashboard_build.tar.gz "$(pwd)/../../../../"

# Clean build directory (already compressed above)
rm -rf build
rm -rf node_modules

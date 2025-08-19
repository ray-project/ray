#!/bin/bash
set -euxo pipefail

# Host user UID/GID
HOST_UID=${HOST_UID:-$(id -u)}
HOST_GID=${HOST_GID:-$(id -g)}

if [[ "$EUID" -eq 0 ]]; then

  # Install sudo
  yum -y install sudo

  # Create group and user
  groupadd -g "$HOST_GID" builduser
  useradd -m -u "$HOST_UID" -g "$HOST_GID" -d /ray builduser

  # Give sudo access
  echo "builduser ALL=(ALL) NOPASSWD:ALL" > /etc/sudoers.d/builduser
  chmod 0440 /etc/sudoers.d/builduser

  exec sudo -E -u builduser HOME="$HOME" bash "$0" "$@" || {
    echo "Failed to exec as builduser." >&2
    exit 1
  }

fi

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
NODE_DIR="$HOME/nodejs"

mkdir -p "$NODE_DIR"

# Download tarball
curl -fsSLO "${NODE_BASE_URL}/${NODE_TARBALL}"

# Verify checksum for our tarball only
echo "$NODE_SHASUM256  $NODE_TARBALL" | sha256sum -c -

# Extract
tar -xJf "$NODE_TARBALL" -C "$NODE_DIR"
rm -f "$NODE_TARBALL"

# Add Node to PATH for this session
export PATH="$NODE_DIR/node-v${NODE_VERSION_FULL}-linux-${NODE_ARCH}/bin:$PATH"

# Quick sanity check
node -v
npm -v

# -----------------------------
# Build the dashboard
# -----------------------------
cd "$(dirname "$0")/../../python/ray/dashboard/client"

# Clean previous builds (optional)
rm -rf build

# Install and build
npm ci
npm run build

# Archive the output to be used in the wheel build
tar -czf dashboard_build.tar.gz -C build .
mv dashboard_build.tar.gz "$HOME"/dashboard_build.tar.gz

#!/bin/bash
set -euxo pipefail

# Host user UID/GID
HOST_UID=${HOST_UID:-$(id -u)}
HOST_GID=${HOST_GID:-$(id -g)}

if [ "$EUID" -eq 0 ]; then

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
# Secure nvm installation
# -----------------------------
NVM_VERSION="v0.39.7"
NVM_INSTALL_SH="install.sh"
NVM_INSTALL_URL="https://raw.githubusercontent.com/nvm-sh/nvm/${NVM_VERSION}/${NVM_INSTALL_SH}"
NVM_SHA256="8e45fa547f428e9196a5613efad3bfa4d4608b74ca870f930090598f5af5f643"  # Precomputed hash (valid as of Aug 2025)

# Download nvm installer
curl -fsSL -o "$NVM_INSTALL_SH" "$NVM_INSTALL_URL"

# Verify checksum
echo "${NVM_SHA256}  ${NVM_INSTALL_SH}" | sha256sum -c -

# Run installer
bash "$NVM_INSTALL_SH"
rm "$NVM_INSTALL_SH"

# Load nvm into shell
export NVM_DIR="$HOME/.nvm"
# shellcheck source=/dev/null
source "$NVM_DIR/nvm.sh"

# -----------------------------
# Setup Node.js and build dashboard
# -----------------------------
NODE_VERSION="14"
nvm install "$NODE_VERSION"
nvm use "$NODE_VERSION"

# Build the dashboard using Node
cd "$(dirname "$0")/../../python/ray/dashboard/client"

# Clean previous builds (optional)
rm -rf build

# Install and build
npm ci
npm run build

# Archive the output to be used in the wheel build
tar -czf dashboard_build.tar.gz -C build .
mv dashboard_build.tar.gz /ray/dashboard_build.tar.gz

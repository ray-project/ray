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
  echo "builduser ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

  exec sudo -E -u builduser HOME="$HOME" bash "$0" "$@"

  exit 0

fi

# Build the dashboard using Node
cd "$(dirname "$0")/../../python/ray/dashboard/client"

# Clean previous builds (optional)
rm -rf build

# Install and build
npm ci
npm run build

# Archive the output to be used in the wheel build
tar -czf dashboard_build.tar.gz -C build .
mv dashboard_build.tar.gz ../../../../../dashboard_build.tar.gz

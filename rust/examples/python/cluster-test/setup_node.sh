#!/bin/bash
set -ex

# Install system dependencies
sudo apt-get update -qq
sudo apt-get install -y -qq build-essential pkg-config libssl-dev protobuf-compiler python3-pip python3-venv cmake redis-server git curl

# Install Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source "$HOME/.cargo/env"
rustc --version

# Install Ray Python (latest release)
pip3 install --quiet ray[default]

echo "=== Setup complete ==="
ray --version

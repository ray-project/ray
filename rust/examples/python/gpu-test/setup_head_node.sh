#!/bin/bash
set -ex

# Head node setup for GPU experiment.
# Instance: t3.large (or similar), Ubuntu 22.04 LTS
# This node runs: GCS server, Raylet, and the GPU driver script.

# Install system dependencies
sudo apt-get update -qq
sudo apt-get install -y -qq \
    build-essential pkg-config libssl-dev cmake git curl \
    python3-pip python3-venv

# Install protoc 25.1 (Ubuntu 22.04 ships 3.12 which lacks proto3 optional support)
PROTOC_VERSION=25.1
curl -sSL "https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/protoc-${PROTOC_VERSION}-linux-x86_64.zip" -o /tmp/protoc.zip
sudo unzip -o /tmp/protoc.zip -d /usr/local bin/protoc
sudo unzip -o /tmp/protoc.zip -d /usr/local 'include/*'
rm /tmp/protoc.zip
protoc --version

# Install Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source "$HOME/.cargo/env"
rustc --version

# Install maturin (PyO3 build tool)
pip3 install --quiet maturin

# Build Rust binaries (GCS server + Raylet)
echo "=== Building Rust binaries ==="
cd ~/ray/rust
cargo build --release -p ray-raylet -p ray-gcs
ls -lh target/release/gcs_server target/release/raylet

# Build PyO3 wheel (_raylet.so)
echo "=== Building _raylet wheel ==="
cd ~/ray/rust/ray-core-worker-pylib
maturin build --release --features python
ls -lh ../target/wheels/*.whl

# Install the wheel locally
pip3 install ../target/wheels/*.whl --force-reinstall
python3 -c "from _raylet import PyCoreWorker; print('_raylet import OK')"

echo ""
echo "=== Head node setup complete ==="
echo ""
echo "Next steps:"
echo "  1. SCP the wheel to the GPU node:"
echo "     scp ~/ray/rust/target/wheels/*.whl ubuntu@<GPU_NODE_IP>:~/"
echo "  2. SCP the worker scripts to the GPU node:"
echo "     scp ~/ray/rust/examples/python/gpu-test/gpu_worker.py ubuntu@<GPU_NODE_IP>:~/"
echo "     scp ~/ray/rust/examples/python/gpu-test/start_gpu_workers.sh ubuntu@<GPU_NODE_IP>:~/"
echo "     scp ~/ray/rust/examples/python/gpu-test/setup_gpu_node.sh ubuntu@<GPU_NODE_IP>:~/"

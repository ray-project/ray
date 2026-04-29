#!/bin/bash
# This script is used to build an extra layer on top of the base llm image
# to run the llm sglang release tests

set -exo pipefail

# Install rust via rustup (apt's rustc 1.75 is too old for crates requiring edition2024, e.g. idna_adapter)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable --profile minimal
export PATH="$HOME/.cargo/bin:$PATH"

pip3 uninstall -y vllm
pip3 install "sglang[all,ray]==0.5.10rc0"
# Reinstall opentelemetry-proto to regenerate _pb2.py files compatible with
# the protobuf version that sglang pulls in (protobuf 4.x+ removed old-style
# descriptor creation, causing Ray dashboard to crash on startup).
pip3 install --upgrade opentelemetry-proto opentelemetry-sdk opentelemetry-exporter-prometheus

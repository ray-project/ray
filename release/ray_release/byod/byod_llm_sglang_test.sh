#!/bin/bash
# This script is used to build an extra layer on top of the base llm image
# to run the llm sglang release tests

set -exo pipefail

# Install rust
sudo apt-get update && sudo apt-get install -y rustc cargo && sudo rm -rf /var/lib/apt/lists/*

pip3 uninstall -y vllm
pip3 install "sglang[all,ray]==0.5.10rc0"
# Reinstall opentelemetry-proto to regenerate _pb2.py files compatible with
# the protobuf version that sglang pulls in (protobuf 4.x+ removed old-style
# descriptor creation, causing Ray dashboard to crash on startup).
pip3 install --upgrade opentelemetry-proto opentelemetry-sdk opentelemetry-exporter-prometheus

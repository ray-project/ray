#!/bin/bash
# This script is used to build an extra layer on top of the base llm image
# to run the llm sglang release tests

set -exo pipefail
pip3 uninstall -y vllm
pip3 install "sglang[all,ray] @ git+https://github.com/sgl-project/sglang.git@c0172aef6eabca1eb1a8ac9b359f57cd7a0490e8#subdirectory=python"
# Reinstall opentelemetry-proto to regenerate _pb2.py files compatible with
# the protobuf version that sglang pulls in (protobuf 4.x+ removed old-style
# descriptor creation, causing Ray dashboard to crash on startup).
pip3 install --upgrade opentelemetry-proto opentelemetry-sdk opentelemetry-exporter-prometheus

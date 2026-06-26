#!/bin/bash
# This script is used to build an extra layer on top of the base llm image
# to run the llm sglang release tests

set -exo pipefail

# Install build deps for Rust crates linking against OpenSSL (e.g. openssl-sys via reqwest)
sudo apt-get update && sudo apt-get install -y --no-install-recommends pkg-config libssl-dev

# Install rust via rustup (apt's rustc 1.75 is too old for crates requiring edition2024, e.g. idna_adapter)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable --profile minimal
export PATH="$HOME/.cargo/bin:$PATH"

# sglang 0.5.10 pins outlines==0.1.11 -> outlines_core (0.1.x), which ships no
# Python 3.14 wheels and must be built from source. Its Rust extension uses
# PyO3 0.22.6, whose hard max is Python 3.13, so the build aborts with
# "the configured Python interpreter version (3.14) is newer than PyO3's
# maximum supported version (3.13)". Forward-compat mode makes PyO3 build
# against the stable ABI (abi3) so the resulting extension loads on 3.14.
# See https://github.com/PyO3/pyo3/issues/5505
export PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1

pip3 uninstall -y vllm
pip3 install "sglang[tracing,ray]==0.5.10"
# Reinstall opentelemetry-proto to regenerate _pb2.py files compatible with
# the protobuf version that sglang pulls in (protobuf 4.x+ removed old-style
# descriptor creation, causing Ray dashboard to crash on startup).
pip3 install --upgrade opentelemetry-proto opentelemetry-sdk opentelemetry-exporter-prometheus

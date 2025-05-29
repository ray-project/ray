#!/bin/bash
# This script is used to build an extra layer on top of the base llm image
# to install vllm at specific version that includes necessary changes for
# PD-disaggregated serving.

set -exo pipefail

# https://github.com/vllm-project/vllm/pull/17751 (Nixl Integration. May 12)
pip3 install --no-cache-dir \
    "vllm==0.9.0"

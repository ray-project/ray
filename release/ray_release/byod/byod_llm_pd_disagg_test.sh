#!/bin/bash
# This script is used to build an extra layer on top of the base llm image
# to install vllm at specific version that includes necessary changes for
# PD-disaggregated serving.

set -exo pipefail

# https://github.com/vllm-project/vllm/pull/17751 (Nixl Integration. May 12)
pip3 install --no-cache-dir \
    "vllm@https://wheels.vllm.ai/d19110204c03e9b77ed957fc70c1262ff370f5e2/vllm-1.0.0.dev-cp38-abi3-manylinux1_x86_64.whl"

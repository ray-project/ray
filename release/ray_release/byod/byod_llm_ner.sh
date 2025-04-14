#!/bin/bash

set -exo pipefail

# Install Python dependencies
pip3 install --no-cache-dir --upgrade \
    "ray[serve,llm]>=2.44.0" \
    "vllm>=0.7.2" \
    "xgrammar==0.1.11" \
    "pynvml==12.0.0" \
    "hf_transfer==0.1.9" \
    "tensorboard" \
    "git+https://github.com/hiyouga/LLaMA-Factory.git#egg=llamafactory"

# Env vars
export HF_HUB_ENABLE_HF_TRANSFER=1

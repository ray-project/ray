#!/bin/bash

set -exo pipefail

# Will use lockfile instead later
# pip3 install --no-cache-dir -r https://raw.githubusercontent.com/anyscale/e2e-llm-workflows/refs/heads/main/lockfile.txt

# Install Python dependencies
pip3 install --no-cache-dir \
    "xgrammar==0.1.11" \
    "pynvml==12.0.0" \
    "hf_transfer==0.1.9" \
    "tensorboard==2.19.0" \
    "git+https://github.com/hiyouga/LLaMA-Factory.git@v0.9.4#egg=llamafactory"


# Env vars
export HF_HUB_ENABLE_HF_TRANSFER=1

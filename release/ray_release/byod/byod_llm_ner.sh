#!/bin/bash

set -exo pipefail

# Install Python dependencies
pip3 install --no-cache-dir \
    "xgrammar==0.1.11" \
    "pynvml==12.0.0" \
    "hf_transfer==0.1.9" \
    "tensorboard==2.19.0" \
    "git+https://github.com/hiyouga/LLaMA-Factory.git@ac8c6fdd3ab7fb6372f231f238e6b8ba6a17eb16#egg=llamafactory"

# Env vars
export HF_HUB_ENABLE_HF_TRANSFER=1

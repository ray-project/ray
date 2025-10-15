#!/bin/bash

set -exo pipefail

# Python dependencies
pip3 install --no-cache-dir \
    "llamafactory@git+https://github.com/hiyouga/LLaMA-Factory.git@v0.9.3" \
    "deepspeed==0.16.9" \
    "wandb==0.21.3" \
    "tensorboard==2.20.0" \
    "mlflow==3.4.0" \
    "bitsandbytes==0.47.0" \
    "autoawq==0.2.9" \
    "flash-attn==2.8.3" \
    "liger-kernel==0.6.2" \
    "hf_transfer==0.1.9"

# Env vars
export HF_HUB_ENABLE_HF_TRANSFER=1

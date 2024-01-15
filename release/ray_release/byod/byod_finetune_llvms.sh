#!/bin/bash
# This script is used to build an extra layer on top of the base anyscale/ray image
# to run the workspace_template_finetuning_llms_with_deepspeed_llama_2_7b test

set -exo pipefail

pip3 install -U \
    torch==2.1.1 \
    deepspeed==0.10.2 \
    fairscale==0.4.13 \
    datasets==2.14.4 \
    accelerate==0.21.0 \
    evaluate==0.4.0 \
    wandb==0.15.8 \
    pytorch-lightning==2.0.6 \
    "protobuf<3.21.0" \
    torchmetrics==1.0.3 \
    sentencepiece==0.1.99 \
    "urllib3<1.27" \
    transformers==4.36.2 \
    peft==0.7.0
pip3 install -U flash-attn==2.4.2 --no-build-isolation

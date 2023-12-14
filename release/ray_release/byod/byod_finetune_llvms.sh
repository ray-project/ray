#!/bin/bash
# This script is used to build an extra layer on top of the base anyscale/ray image
# to run the workspace_template_finetuning_llms_with_deepspeed_llama_2_7b test

set -exo pipefail

pip3 install -U \
    torch==2.1.1 \
    torchvision==0.15.1 \
    torchaudio==2.0.1 \
    deepspeed==0.12.3 \
    fairscale==0.4.13 \
    datasets==2.14.4 \
    accelerate==0.21.0 \
    evaluate==0.4.0 \
    bitsandbytes==0.41.1 \
    wandb==0.15.8 \
    pytorch-lightning==2.0.6 \
    "protobuf<3.21.0" \
    torchmetrics==1.0.3 \
    lm_eval==0.3.0 \
    tiktoken==0.1.2 \
    sentencepiece==0.1.99 \
    "urllib3<1.27" \
    git+https://github.com/huggingface/transformers.git@d0c1aeb \
    git+https://github.com/huggingface/peft.git@08368a1fba16de09756f067637ff326c71598fb3

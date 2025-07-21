#!/bin/bash

set -exo pipefail

# Install Python dependencies
pip3 install --no-cache-dir \
    accelerate==1.7.0 \
    datasets[audio]==2.2.1 \
    flashinfer-python==0.2.2.post1 \
    huggingface-hub[hf_xet]==0.32.6 \
    pydantic==2.9.2 \
    transformers==4.52.4 \
    xgrammar==0.1.19

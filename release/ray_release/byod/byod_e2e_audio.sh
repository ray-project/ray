#!/bin/bash

set -exo pipefail

# Install Python dependencies
pip3 install --no-cache-dir \
    "pytest>=8.3.5" \
    "ruff>=0.11.5" \
    "transformers>=4.51.3" \
    "torchaudio" \
    "datasets[audio]>=3.6.0" \
    "accelerate" \
    "huggingface_hub[hf_xet]" \
    xgrammar \
    pydantic \
    flashinfer-python

#!/bin/bash

set -euxo pipefail

# Install Python dependencies
pip3 install --no-cache-dir \
    "numpy>=1.24.0,<2.0" \
    "torch>=2.0.0" \
    "transformers>=4.35.0" \
    "accelerate>=0.25.0" \
    "sentencepiece>=0.1.99" \
    "httpx>=0.25.0" \
    "aioboto3>=12.0.0" \
    "pillow>=10.0.0"


sudo apt-get update && sudo apt-get install -y --no-install-recommends \
    ffmpeg \
    && sudo rm -rf /var/lib/apt/lists/*

export S3_BUCKET=anyscale-example-video-analysis-test-bucket

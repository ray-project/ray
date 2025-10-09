#!/bin/bash

set -exo pipefail

# Will use lockfile instead later
# pip3 install --no-cache-dir -r https://raw.githubusercontent.com/anyscale/e2e-llm-workflows/refs/heads/main/lockfile.txt

# Install Python dependencies
pip3 install --no-cache-dir \
    "torch==2.8.0" \
    "torchvision==0.23.0" \
    "matplotlib==3.10.6" \
    "pyarrow==14.0.2" \
    "datasets==2.19.2" \
    "lightning==2.5.5" \
    "scikit-learn==1.7.2" \
    "xgboost==3.0.5" \
    "seaborn==0.13.2" \
    "statsmodels==0.14.5" \
    "pycocotools==2.0.10" \
    "transformers==4.56.2" \
    "accelerate==1.10.1"

# Env vars
export RAY_TRAIN_V2_ENABLED=1
# DO NOT hardcode HUGGING_FACE_HUB_TOKEN here — set it in Workspace Secrets instead

#!/bin/bash

set -exo pipefail

# Install Python dependencies
pip3 install --no-cache-dir \
    "matplotlib==3.10.0" \
    "torch==2.7.1" \
    "transformers==4.52.3" \
    "scikit-learn==1.6.0" \
    "mlflow==2.19.0" \
    "ipywidgets==8.1.3"

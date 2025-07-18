#!/bin/bash

# Set bash options for safer script execution:
# -e: Exit immediately if any command fails
# -x: Print each command before executing it (for debugging)
# -o pipefail: Fail if any command in a pipeline fails (not just the last one)
set -exo pipefail

# Install Python dependencies.
pip3 install --no-cache-dir \
    aiohttp==3.11.16 \
    nbformat==5.9.2 \
    numpy==1.26.4 \
    pandas==2.3.0 \
    pyyaml==6.0.1 \
    s3fs==2023.5.0 \
    scikit-learn==1.3.2 \
    torch==2.3.0

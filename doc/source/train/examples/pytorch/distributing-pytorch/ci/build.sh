#!/bin/bash

set -exo pipefail

# Install Python dependencies
pip3 install --no-cache-dir \
    "torch==2.7.0" \
    "torchvision==0.22.0"

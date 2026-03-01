#!/bin/bash
set -exo pipefail

pip3 install --no-cache-dir \
    "torch==2.8.0" \
    "torchvision==0.23.0" \
    "matplotlib==3.10.6" \
    "pyarrow==14.0.2"

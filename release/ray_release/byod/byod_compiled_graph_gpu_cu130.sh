#!/bin/bash
set -exo pipefail

pip3 install --no-cache-dir \
    "torch==2.9.1" \
    "cupy-cuda13x" \
    --extra-index-url https://download.pytorch.org/whl/cu130

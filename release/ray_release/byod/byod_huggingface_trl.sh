#!/bin/bash
# This script is used to build an extra layer on top of the base anyscale/ray image
# to run the Hugging Face TRL example release test.

set -exo pipefail

# Install TRL and math_verify
pip3 install --no-cache-dir "trl[vllm]" math_verify
pip3 install --no-cache-dir --force-reinstall numpy pandas tf_keras
# vllm requires numpy>=2.x; upgrade tensorflow and matplotlib to a version compatible with numpy 2.x.
pip3 install --no-cache-dir --upgrade tensorflow matplotlib
# wandb and comet were installed but didn't have API keys which raised errors and warnings
pip3 uninstall wandb comet

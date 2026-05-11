#!/bin/bash
# This script is used to build an extra layer on top of the base anyscale/ray image
# to run the Hugging Face TRL example release test.

set -exo pipefail

# Install TRL and math_verify
# vllm>=0.18.1 fixes the standalone_compile FakeTensorMode mock.patch failure on Python <= 3.10 (vllm-project/vllm#37158)
pip3 install --no-cache-dir "trl[vllm]" "vllm>=0.18.1" math_verify
pip3 install --no-cache-dir --force-reinstall numpy pandas
# vllm requires numpy>=2.x; upgrade matplotlib to a version compatible with numpy 2.x.
pip3 install --no-cache-dir --upgrade matplotlib
# `transformers` auto-imports TF when present and TF's bundled grpc collides with Ray's grpc on the `grpc_experiments` absl flag
pip3 uninstall -y wandb comet_ml tensorflow tf_keras

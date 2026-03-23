#!/bin/bash
# This script is used to build an extra layer on top of the base anyscale/ray image
# to run the Hugging Face TRL example release test.

set -exo pipefail

# Install TRL and math_verify
pip3 install --no-cache-dir "trl[vllm]" math_verify

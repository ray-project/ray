#!/bin/bash
# This script is used to build an extra layer on top of the base anyscale/ray image
# to run the Hugging Face TRL example release test.

set -exo pipefail

# Update accelerate version
pip3 install --no-cache-dir --upgrade "trl[vllm]" numpy pandas math_verify

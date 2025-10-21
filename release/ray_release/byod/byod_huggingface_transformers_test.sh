#!/bin/bash
# This script is used to build an extra layer on top of the base anyscale/ray image
# to run the Hugging Face Transformers release test.

set -exo pipefail

# Update accelerate version
pip install accelerate==0.32.0

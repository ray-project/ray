#!/bin/bash
# This script is used to build an extra layer on top of the base anyscale/ray image 
# to run the agent stress test.

set -exo pipefail

pip3 install torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cu118
pip3 install "pytorch_lightning==2.0.2" "transformers==4.29.2" "accelerate==0.19.0"

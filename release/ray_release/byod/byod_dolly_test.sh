#!/bin/bash
# This script is used to build an extra layer on top of the base anyscale/ray image 
# to run the agent stress test.

set -exo pipefail

pip3 uninstall -y pytorch-lightning
pip3 install torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cu118
pip3 install lightning==2.0.3 myst-parser==1.0.0 myst-nb==1.1.0

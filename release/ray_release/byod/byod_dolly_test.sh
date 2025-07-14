#!/bin/bash
# This script is used to build an extra layer on top of the base anyscale/ray image
# to run the agent stress test.

set -exo pipefail

pip3 install -c "$HOME/requirements_compiled.txt" myst-parser myst-nb

pip3 uninstall -y pytorch-lightning
pip3 install torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cu118

# TODO(matthewdeng): upgrade datasets globally
pip3 install lightning==2.0.3 datasets==3.6.0

#!/bin/bash
# This script is used to build an extra layer on top of the base anyscale/ray image
# to run the PyTorch Lightning release test.

set -exo pipefail

# Replace pytorch-lightning with lightning
pip3 uninstall -y pytorch-lightning
pip3 install lightning==2.4.0

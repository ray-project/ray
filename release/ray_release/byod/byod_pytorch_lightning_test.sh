#!/bin/bash
# This script is used to build an extra layer on top of the base anyscale/ray image
# to run the PyTorch Lightning release test.

set -exo pipefail

# Install Lightning (the new unified package that includes pytorch-lightning)
pip3 install lightning==2.4.0

#!/bin/bash
# This script is used to build an extra layer on top of the base anyscale/ray image 
# to run the agent stress test.

set -exo pipefail

pip3 install -U --force-reinstall pytorch-lightning lightning-bolts
pip uninstall ray_lightning -y # Uninstall first so pip does a reinstall.
pip3 install -U --no-cache-dir git+https://github.com/ray-project/ray_lightning#ray_lightning
pip3 install --force-reinstall torch==1.11.0
pip3 install --force-reinstall torchvision==0.12.0

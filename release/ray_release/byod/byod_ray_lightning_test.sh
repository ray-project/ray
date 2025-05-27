#!/bin/bash
# This script is used to build an extra layer on top of the base anyscale/ray image
# to run the agent stress test.

set -exo pipefail

pip3 install -U --force-reinstall ray-lightning pytorch-lightning lightning-bolts
pip3 install --force-reinstall torch==1.11.0
pip3 install --force-reinstall torchvision==0.12.0

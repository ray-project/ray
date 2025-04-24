#!/bin/bash
# This script is used to build an extra layer on top of the base anyscale/ray image
# to run stable diffusion test.

set -exo pipefail

pip3 install transformers==4.31.0 diffusers==0.21.3

#!/bin/bash
# This script is used to build an extra layer on top of the base anyscale/ray image 
# to run stable diffusion test.

set -exo pipefail

pip3 install transformers==4.36.2 diffusers==0.25.1

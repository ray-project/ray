#!/bin/bash
# This script is used to build an extra layer on top of the base anyscale/ray image 
# to run the horovod tests
#
# TODO(can): once the library versions can be resolved cleanly with other dependencies,
# they should be managed through requirements_byod.in file

set -exo pipefail

pip3 install cupy-cuda113 numpy==1.21.0 protobuf==3.20.0

pip3 install --upgrade pip
# Install Alpa from source for now.
# TODO(jungong) : pip install alpa after next release.
git clone https://github.com/alpa-projects/alpa.git
pip3 install -e alpa
pip3 install -e alpa/examples
# Install custom built jaxlib.
pip install jaxlib==0.3.22+cuda113.cudnn820 -f https://alpa-projects.github.io/wheels.html
# Install nvidia dependencies.
pip3 install --no-cache-dir nvidia-pyindex
pip3 install --no-cache-dir nvidia-tensorrt==7.2.3.4
# Huggingface transformers.
# TODO(jungong) : bring llm_serving up to date with latest transforemrs library. 
pip install -U transformers==4.23.1
pip install -U accelerate

#!/bin/bash

set -e

# shellcheck disable=SC2139
alias pip="$HOME/anaconda3/bin/pip"

sudo apt-get update \
    && sudo apt-get install -y gcc \
        cmake \
        libgtk2.0-dev \
        zlib1g-dev \
        libgl1-mesa-dev \
        unzip \
        unrar

pip --no-cache-dir install -U pip pip-tools

# Install requirements
pip --no-cache-dir install -U -r requirements.txt

# Install other requirements. Keep pinned requirements bounds as constraints
pip --no-cache-dir install -U \
           -c requirements.txt \
           -c requirements_compiled.txt \
           -r core-requirements.txt \
           -r data-requirements.txt \
           -r rllib-requirements.txt \
           -r rllib-test-requirements.txt \
           -r train-requirements.txt \
           -r train-test-requirements.txt \
           -r tune-requirements.txt \
           -r tune-test-requirements.txt \
           -r ray-docker-requirements.txt


# Remove any device-specific constraints from requirements_compiled.txt.
# E.g.: torch-scatter==2.1.1+pt20cpu or torchvision==0.15.2+cpu
# These are replaced with gpu-specific requirements in dl-gpu-requirements.txt.
sed "/[0-9]\+cpu/d;/[0-9]\+pt/d" "requirements_compiled.txt" > requirements_compiled_gpu.txt

# explicitly install (overwrite) pytorch with CUDA support
pip --no-cache-dir install \
           -c requirements.txt \
           -c requirements_compiled_gpu.txt \
           -r dl-gpu-requirements.txt

sudo apt-get clean

sudo rm ./*requirements*.txt

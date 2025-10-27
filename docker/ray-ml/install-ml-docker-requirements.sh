#!/bin/bash

set -e

# shellcheck disable=SC2139
alias pip="$HOME/anaconda3/bin/pip"

sudo apt-get update \
    && sudo apt-get install -y gcc \
        cmake \
        libgtk2.0-dev \
        libgl1-mesa-dev \
        libgl1-mesa-glx \
        libosmesa6 \
        libosmesa6-dev \
        libglfw3 \
        patchelf \
        unzip \
        unrar \
        zlib1g-dev

# Install requirements
pip --no-cache-dir install -r requirements.txt -c requirements_compiled.txt

# Install other requirements. Keep pinned requirements bounds as constraints
pip --no-cache-dir install \
           -c requirements.txt \
           -c requirements_compiled.txt \
           -r dl-cpu-requirements.txt \
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

# requirements_compiled.txt will be kept.
sudo rm ./*requirements.txt requirements_compiled_gpu.txt

# MuJoCo Installation.
export MUJOCO_GL=osmesa
wget https://github.com/google-deepmind/mujoco/releases/download/2.1.1/mujoco-2.1.1-linux-x86_64.tar.gz
mkdir -p ~/.mujoco
mv mujoco-2.1.1-linux-x86_64.tar.gz ~/.mujoco/.
cd ~/.mujoco || exit
tar -xf ~/.mujoco/mujoco-2.1.1-linux-x86_64.tar.gz
export LD_LIBRARY_PATH=${LD_LIBRARY_PATH:-}:/root/.mujoco/mujoco-2.1.1/bin

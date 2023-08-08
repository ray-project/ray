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

# MuJoCo Installation.
export MUJOCO_GL=osmesa
wget https://mujoco.org/download/mujoco210-linux-x86_64.tar.gz
mkdir -p ~/.mujoco
mv mujoco210-linux-x86_64.tar.gz ~/.mujoco/.
cd ~/.mujoco || exit
tar -xf ~/.mujoco/mujoco210-linux-x86_64.tar.gz
export LD_LIBRARY_PATH=${LD_LIBRARY_PATH:-}:/root/.mujoco/mujoco210/bin

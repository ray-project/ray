#!/bin/bash
# This script is used to build an extra layer on top of the base anyscale/ray image 
# to run the agent stress test.

set -exo pipefail

git clone https://github.com/ray-project/rl-experiments.git
unzip rl-experiments/halfcheetah-sac/2022-12-17/halfcheetah_1500_mean_reward_sac.zip -d ~/.
pip3 install torch==2.0.0+cu118 torchvision==0.15.1+cu118 --index-url https://download.pytorch.org/whl/cu118

# TODO(sven): remove once nightly image gets gymnasium and the other new dependencies.
wget https://mujoco.org/download/mujoco210-linux-x86_64.tar.gz
mkdir ~/.mujoco
mv mujoco210-linux-x86_64.tar.gz ~/.mujoco/.
cd ~/.mujoco
tar -xf ~/.mujoco/mujoco210-linux-x86_64.tar.gz

# not strictly necessary, but makes debugging easier
git clone https://github.com/ray-project/ray.git

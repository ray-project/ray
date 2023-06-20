#!/bin/bash
# This script is used to build an extra layer on top of the base anyscale/ray image 
# to run the agent stress test.

set -exo pipefail

git clone https://github.com/ray-project/rl-experiments.git
unzip rl-experiments/halfcheetah-sac/2022-12-17/halfcheetah_1500_mean_reward_sac.zip -d ~/.
# Use torch+CUDA10.2 for our release tests. CUDA11.x has known performance issues in combination with torch+GPU+CNNs
# TODO(sven): remove once nightly image gets upgraded.
pip3 install torch==1.12.1+cu102 torchvision==0.13.1+cu102 --extra-index-url https://download.pytorch.org/whl/cu102

# TODO(sven): remove once nightly image gets gymnasium and the other new dependencies.
wget https://mujoco.org/download/mujoco210-linux-x86_64.tar.gz
mkdir ~/.mujoco
mv mujoco210-linux-x86_64.tar.gz ~/.mujoco/.
cd ~/.mujoco
tar -xf ~/.mujoco/mujoco210-linux-x86_64.tar.gz
echo 'export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/home/ray/.mujoco/mujoco210/bin' >> /home/ray/.bashrc

# not strictly necessary, but makes debugging easier
git clone https://github.com/ray-project/ray.git

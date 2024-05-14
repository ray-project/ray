#!/bin/bash
# This script is used to build an extra layer on top of the base anyscale/ray image 
# to run the agent stress test.

set -exo pipefail


# TODO: https://github.com/Farama-Foundation/AutoROM/issues/48
pip3 install https://ray-ci-deps-wheels.s3.us-west-2.amazonaws.com/AutoROM.accept_rom_license-0.5.4-py3-none-any.whl
git clone https://github.com/ray-project/rl-experiments.git
unzip rl-experiments/halfcheetah-sac/2022-12-17/halfcheetah_1500_mean_reward_sac.zip -d ~/.
pip3 uninstall -y minigrid
pip3 install werkzeug==2.3.8

# not strictly necessary, but makes debugging easier
git clone https://github.com/ray-project/ray.git

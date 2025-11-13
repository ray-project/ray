#!/bin/bash

# install python package
pip install jsonpatch
pip install "ray[default]==2.51.0"
pip uninstall -y ray

source /root/.bashrc
set -exuo pipefail

export RAY_INSTALL_JAVA="${RAY_INSTALL_JAVA:-0}"

# Add the repo folder to the safe.directory global variable to avoid the failure
# because of security check from git, when executing the following command
# `git clean ...`,  while building wheel locally.
git config --global --add safe.directory /ray

# Setup runtime environment
./ci/build/build-manylinux-forge.sh
source "$HOME"/.nvm/nvm.sh

# Compile ray
./ci/build/build-manylinux-ray.sh
git clean -f -f -x -d -e .whl -e python/ray/dashboard/client -e dashboard/client -e python/ray/jars -e python/requirements_compiled.txt
./ci/build/build-manylinux-wheel.sh "cp310-cp310"

# Build ray jar
./ci/build/build-manylinux-jar.sh

# Ray[data] unit tests
ray_dir=$(dirname "$(readlink -f "$0")")
export PYTHONPATH="$ray_dir"

# Spot-Autoscaler unit tests
pytest ./python/ray/autoscaler/v2/tests
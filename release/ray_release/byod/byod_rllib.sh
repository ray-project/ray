#!/bin/bash

set -exo pipefail

# torch/torchvision come from the CUDA (cu128) wheel index and intentionally differ
# from requirements_compiled; keep them a separate, unconstrained install.
pip3 install -U torch==2.7 torchvision==0.22 --index-url https://download.pytorch.org/whl/cu128

# Everything else is pinned in requirements_compiled — install under its constraints so
# the byod image cannot drift from the compiled set. (Drift is what caused the RLlib
# Pong crash: byod had opencv 4.9.0.80 while requirements_compiled had the numpy-2-safe
# 4.10.0.84.)
pip3 install -U -c "$HOME/requirements_compiled.txt" \
  "gymnasium[mujoco]" ale_py imageio opencv-python-headless pettingzoo pygame wandb

#!/bin/bash
# BYOD script for installing vLLM with CPU support for Ray Serve LLM CPU-only mode.
#
# This script installs the vLLM CPU wheel which enables serving LLMs on CPU-only clusters.
# Reference: https://docs.vllm.ai/en/latest/getting_started/installation/cpu.html

set -exo pipefail

# The base image has GPU vLLM installed, so we need to force-reinstall to
# replace it with the CPU variant. However, pip's --force-reinstall also reinstalls
# all dependencies, which would replace the dev Ray (3.0.0.dev0) with a release
# version and cause CI validation to fail (commit hash mismatch).
#
# Solution: back up the dev Ray as a wheel, install vLLM (which replaces Ray),
# then restore the dev Ray.

# Back up the dev Ray wheel before vLLM replaces it
pip wheel ray --no-deps -w /tmp/ray_wheel

# Force reinstall vLLM CPU variant (this also reinstalls deps including Ray)
VLLM_USE_PRECOMPILED=1 \
VLLM_PRECOMPILED_WHEEL_VARIANT=cpu \
VLLM_TARGET_DEVICE=cpu \
pip install --force-reinstall vllm==0.13.0 --extra-index-url https://download.pytorch.org/whl/cpu

# Restore the dev Ray version
pip install /tmp/ray_wheel/ray*.whl --force-reinstall --no-deps

# Verify the installation
python -c "import vllm; print(f'vLLM version: {vllm.__version__}')"

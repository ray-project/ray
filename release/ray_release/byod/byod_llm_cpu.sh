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
# Solution: back up the dev Ray wheel from the image's .whl directory,
# install vLLM (which replaces Ray), then restore the dev Ray.

mkdir -p /tmp/ray_wheel

# Find and copy the dev Ray wheel from the image's .whl directory
# The wheel is stored at /home/ray/.whl/ during the image build process
RAY_WHEEL=""
for dir in /home/ray/.whl ~/.whl .whl; do
    if [ -d "$dir" ]; then
        RAY_WHEEL=$(find "$dir" -maxdepth 1 -name 'ray-*.whl' -print -quit)
        if [ -n "$RAY_WHEEL" ]; then
            echo "Found Ray wheel at: $RAY_WHEEL"
            cp "$RAY_WHEEL" /tmp/ray_wheel/
            break
        fi
    fi
done

if [ -z "$RAY_WHEEL" ]; then
    echo "ERROR: Could not find Ray wheel in .whl directory"
    echo "Searched: /home/ray/.whl, ~/.whl, .whl"
    exit 1
fi

# Force reinstall vLLM CPU variant (this also reinstalls deps including Ray)
VLLM_USE_PRECOMPILED=1 \
VLLM_PRECOMPILED_WHEEL_VARIANT=cpu \
VLLM_TARGET_DEVICE=cpu \
pip install --force-reinstall vllm==0.13.0 --extra-index-url https://download.pytorch.org/whl/cpu

# Restore the dev Ray version
pip install /tmp/ray_wheel/ray-*.whl --force-reinstall --no-deps

# Install pytest (not included in base runtime image)
pip install pytest

# Verify the installation
python -c "import vllm; print(f'vLLM version: {vllm.__version__}')"

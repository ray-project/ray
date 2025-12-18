#!/bin/bash
# BYOD script for installing vLLM with CPU support for Ray Serve LLM CPU-only mode.
#
# This script installs the vLLM CPU wheel which enables serving LLMs on CPU-only clusters.
# Reference: https://docs.vllm.ai/en/latest/getting_started/installation/cpu.html

set -exo pipefail

# Install vLLM with CPU support using the precompiled wheel
# This uses the official vLLM CPU wheel from PyPI
VLLM_USE_PRECOMPILED=1 \
VLLM_PRECOMPILED_WHEEL_VARIANT=cpu \
VLLM_TARGET_DEVICE=cpu \
pip install --force-reinstall vllm --extra-index-url https://download.pytorch.org/whl/cpu

# Verify the installation
python -c "import vllm; print(f'vLLM version: {vllm.__version__}')"

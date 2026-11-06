#!/bin/bash
# Dependencies for the DeepSpeed LLM training benchmark
# (release/train_tests/benchmark, deepspeed adapter).
#
# Layered onto the gpu-cu130 BYOD image (anyscale/ray + CUDA 13), with torch
# pinned by the gpu_cu130_py3.10.lock python_depset. Do NOT reinstall torch
# here: DeepSpeed JIT-compiles its ops at runtime against the image's CUDA
# toolkit and the depset's torch build, and a mismatched torch wheel breaks
# that.
#
# transformers must be >= 4.51.0 for Qwen3 (`model_type: qwen3`).

set -exo pipefail

pip3 install --no-cache-dir \
  "transformers>=4.51.0" \
  "datasets>=3.0" \
  "nvidia-ml-py>=12.0.0"
pip3 install --no-cache-dir --no-build-isolation "deepspeed==0.19.2"

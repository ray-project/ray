#!/bin/bash
# Dependencies for the DeepSpeed LLM training benchmark
# (release/train_tests/benchmark, deepspeed adapter).
#
# Installed on top of the ray-ml GPU image's existing CUDA-matched torch. Do NOT
# reinstall torch here: DeepSpeed JIT-compiles ops against the image's CUDA
# toolkit, and a mismatched torch wheel breaks that build.
#
# transformers must be >= 4.51.0 for Qwen3 (`model_type: qwen3`).

set -exo pipefail

pip3 install --no-cache-dir \
  "transformers>=4.51.0" \
  "datasets>=3.0" \
  "nvidia-ml-py>=12.0.0"
pip3 install --no-cache-dir --no-build-isolation "deepspeed>=0.14.0"

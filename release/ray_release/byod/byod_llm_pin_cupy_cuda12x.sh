#!/bin/bash

set -euo pipefail

# The LLM BYOD extra-testdeps lock can resolve a newer cupy-cuda12x than the
# ray-llm base image (see python/deplocks/llm/rayllm_py311_cu128.lock /
# rayllm_py312_cu130.lock). That breaks Ray's NCCL collectives, which import
# CuPy. Re-pin to the same version as the LLM base after BYOD pip install.
pip3 install --no-cache-dir "cupy-cuda12x==13.6.0"

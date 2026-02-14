#!/bin/bash

set -euo pipefail

# Install bazel
bash ci/env/install-bazel.sh
export PATH="$HOME/bin:$PATH"

# Compile LLM dependencies
bash ci/ci.sh compile_pip_dependencies
bash ci/compile_llm_requirements.sh
bazel run //ci/raydepsets:raydepsets -- build --all-configs
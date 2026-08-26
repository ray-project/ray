#!/bin/bash
# Builds the constraints file for the ray-llm deplocks: Ray's compiled
# requirements, minus the GPU index directives, minus the pins that are too low
# for vLLM. Dropping a pin lets uv resolve that package freely; vLLM's own
# requirement then sets the floor when it enters at the expand step.
#
# Same header stripping as remove-compiled-headers.sh, but writes its own _llm
# file since every other depsets config constrains on the shared one, and reads
# the source directly since pre_hook order isn't guaranteed.
#
# Usage: strip-llm-constraints.sh <python-version>   e.g. 3.13

set -euo pipefail

PYTHON_VERSION=${1:-}

if [[ -z "${PYTHON_VERSION}" ]]; then
    echo "Usage: $0 <python-version>" >&2
    exit 1
fi

SOURCE_FILE="python/requirements_compiled_py${PYTHON_VERSION}.txt"
OUTPUT_FILE="/tmp/ray-deps/requirements_compiled_py${PYTHON_VERSION}_llm.txt"

mkdir -p /tmp/ray-deps

# click: vLLM needs >=8.4.2 (via huggingface-hub); the compiled pin is 8.3.2 and
# Ray itself only asks for >=7.0.
sed \
    -e '/^--extra-index-url /d' \
    -e '/^--find-links /d' \
    -e '/^click==/d' \
    "$SOURCE_FILE" > "$OUTPUT_FILE"

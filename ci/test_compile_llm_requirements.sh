#!/bin/bash

set -e

# Install uv and set up Python
pip install uv
uv python install 3.11
uv python pin 3.11

# Create a temporary directory for backup files and setup cleanup trap
TEMP_DIR=$(mktemp -d)
cleanup() {
    echo "Cleaning up temporary directory: $TEMP_DIR"
    rm -rf "$TEMP_DIR"
}
trap cleanup EXIT

echo "Created temporary directory: $TEMP_DIR"

# Create backup copies of req files to reference to
cp ./python/requirements_compiled_rayllm_py311_cpu.txt "$TEMP_DIR/requirements_compiled_rayllm_py311_cpu_backup.txt"
cp ./python/requirements_compiled_rayllm_py311_cu121.txt "$TEMP_DIR/requirements_compiled_rayllm_py311_cu121_backup.txt"
cp ./python/requirements_compiled_rayllm_py311_cu128.txt "$TEMP_DIR/requirements_compiled_rayllm_py311_cu128_backup.txt"

./ci/compile_llm_requirements.sh

# Copy files to artifact mount on Buildkite
cp ./python/requirements_compiled_rayllm_py311_cpu.txt /artifact-mount/
cp ./python/requirements_compiled_rayllm_py311_cu121.txt /artifact-mount/
cp ./python/requirements_compiled_rayllm_py311_cu128.txt /artifact-mount/

# Check all files and print if files are not up to date
FAILED=0
for VARIANT in cpu cu121 cu128; do
    diff --color -u ./python/requirements_compiled_rayllm_py311_${VARIANT}.txt "$TEMP_DIR/requirements_compiled_rayllm_py311_${VARIANT}_backup.txt" || {
        echo "requirements_compiled_rayllm_py311_${VARIANT}.txt is not up to date. Please download it from Artifacts tab and git push the changes."
        FAILED=1
    }
done
if [[ $FAILED -eq 1 ]]; then
    exit 1
fi

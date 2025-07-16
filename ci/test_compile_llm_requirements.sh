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
LOCK_TYPES=(rayllm_test ray_test ray rayllm)
VARIANTS=(cpu cu121 cu128)

for LOCK_TYPE in "${LOCK_TYPES[@]}"; do
    for VARIANT in "${VARIANTS[@]}"; do
        cp ./python/requirements_compiled_"${LOCK_TYPE}"_py311_"${VARIANT}".txt "$TEMP_DIR/requirements_compiled_${LOCK_TYPE}_py311_${VARIANT}_backup.txt"
    done
done

./ci/compile_llm_requirements.sh

# Copy files to artifact mount on Buildkite
for LOCK_TYPE in "${LOCK_TYPES[@]}"; do
    for VARIANT in "${VARIANTS[@]}"; do
        cp ./python/requirements_compiled_"${LOCK_TYPE}"_py311_"${VARIANT}".txt /artifact-mount/
    done
done

# Check all files and print if files are not up to date
FAILED=0
for LOCK_TYPE in "${LOCK_TYPES[@]}"; do
    for VARIANT in "${VARIANTS[@]}"; do
        diff --color -u ./python/requirements_compiled_"${LOCK_TYPE}"_py311_"${VARIANT}".txt "$TEMP_DIR/requirements_compiled_${LOCK_TYPE}_py311_${VARIANT}_backup.txt" || {
            echo "requirements_compiled_${LOCK_TYPE}_py311_${VARIANT}.txt is not up to date. Please download it from Artifacts tab and git push the changes."
            FAILED=1
        }
    done
done
if [[ $FAILED -eq 1 ]]; then
    exit 1
fi

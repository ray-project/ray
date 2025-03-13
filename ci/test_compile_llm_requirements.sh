#!/bin/bash

set -e

# Install uv and set up Python
pip install uv
uv python install 3.11
uv python pin 3.11

# Create backup copies of req files to reference to
cp ./python/requirements_compiled_rayllm_py311_cpu.txt requirements_compiled_rayllm_py311_cpu_backup.txt
cp ./python/requirements_compiled_rayllm_py311_cu121.txt requirements_compiled_rayllm_py311_cu121_backup.txt
cp ./python/requirements_compiled_rayllm_py311_cu124.txt requirements_compiled_rayllm_py311_cu124_backup.txt

./ci/compile_llm_requirements.sh

# Copy files to artifact mount on Buildkite
cp -f ./python/requirements_compiled_rayllm_py311_cpu.txt /artifact-mount/
cp -f ./python/requirements_compiled_rayllm_py311_cu121.txt /artifact-mount/
cp -f ./python/requirements_compiled_rayllm_py311_cu124.txt /artifact-mount/

# Check all files and print if files are not up to date
FAILED=0
diff ./python/requirements_compiled_rayllm_py311_cpu.txt requirements_compiled_rayllm_py311_cpu_backup.txt || {
    echo "requirements_compiled_rayllm_py311_cpu.txt is not up to date. Please download it from Artifacts tab and git push the changes."
    FAILED=1
}
diff ./python/requirements_compiled_rayllm_py311_cu121.txt requirements_compiled_rayllm_py311_cu121_backup.txt || {
    echo "requirements_compiled_rayllm_py311_cu121.txt is not up to date. Please download it from Artifacts tab and git push the changes."
    FAILED=1
}
diff ./python/requirements_compiled_rayllm_py311_cu124.txt requirements_compiled_rayllm_py311_cu124_backup.txt || {
    echo "requirements_compiled_rayllm_py311_cu124.txt is not up to date. Please download it from Artifacts tab and git push the changes."
    FAILED=1
}
if [[ $FAILED -eq 1 ]]; then
    exit 1
fi

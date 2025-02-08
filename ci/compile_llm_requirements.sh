#!/bin/bash

set -euo pipefail

PYTHON_CODE="$(python -c "import sys; v=sys.version_info; print(f'py{v.major}{v.minor}')")"

echo "--- Compile dependencies for ${PYTHON_CODE}"

PIP_COMPILE=(pip-compile -v --generate-hashes --strip-extras --unsafe-package ray)

# First, extract base test dependencies from the current compiled mono repo one.
# This also expands to the indirect dependencies for this Python version & platform.
echo "--- Compile ray base test dependencies"
"${PIP_COMPILE[@]}" \
	-c "python/requirements_compiled.txt" \
	"python/requirements.txt" \
	"python/requirements/base-test-requirements.txt" \
	-o "python/requirements_compiled_ray_test_${PYTHON_CODE}.txt"

# Second, expand it into LLM test dependencies
echo "--- Compile LLM test dependencies"
"${PIP_COMPILE[@]}" \
	-c "python/requirements_compiled_ray_test_${PYTHON_CODE}.txt" \
	"python/requirements.txt" \
	"python/requirements/base-test-requirements.txt" \
	"python/requirements/llm/llm-requirements.txt" \
	"python/requirements/llm/llm-test-requirements.txt" \
	-o "python/requirements_compiled_rayllm_test_${PYTHON_CODE}.txt"

# Third, extract the ray base dependencies from ray base test dependencies.
# TODO(aslonnie): This should be used for installing ray in the container images.
echo "--- Compile ray base test dependencies"
"${PIP_COMPILE[@]}" \
	-c "python/requirements_compiled_ray_test_${PYTHON_CODE}.txt" \
	"python/requirements.txt" \
	-o "python/requirements_compiled_ray_${PYTHON_CODE}.txt"

# Finally, extract the LLM dependencies from the LLM test dependencies,
# which is also an expansion of the ray base dependencies.
# TODO(aslonnie): This should be used for installing ray[llm] in the container images.
echo "--- Compile LLM dependencies"
"${PIP_COMPILE[@]}" \
	-c "python/requirements_compiled_rayllm_test_${PYTHON_CODE}.txt" \
	"python/requirements.txt" \
	"python/requirements/llm/llm-requirements.txt" \
	-o "python/requirements_compiled_rayllm_${PYTHON_CODE}.txt"

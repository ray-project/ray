#!/bin/bash

set -euo pipefail

PYTHON_CODE="$(python -c "import sys; v=sys.version_info; print(f'py{v.major}{v.minor}')")"
if [[ "${PYTHON_CODE}" != "py311" ]]; then
	echo "--- Python version is not 3.11"
	echo "--- Current Python version: ${PYTHON_CODE}"
	exit 1
fi

for CUDA_CODE in cpu cu121 cu124 ; do
	PYTHON_CUDA_CODE="${PYTHON_CODE}_${CUDA_CODE}"

	echo "--- Compile dependencies for ${PYTHON_CODE}_${CUDA_CODE}"

	UV_PIP_COMPILE=(
		uv pip compile --generate-hashes --strip-extras
		--unsafe-package ray
		# The version we use on python 3.9 is not installable on python 3.11
		--unsafe-package grpcio-tools
		# setuptools should not be pinned.
		--unsafe-package setuptools
		--index-url "https://pypi.org/simple"
		--extra-index-url "https://download.pytorch.org/whl/${CUDA_CODE}"
		--find-links "https://data.pyg.org/whl/torch-2.5.1+${CUDA_CODE}.html"
		--index-strategy unsafe-best-match
		--no-strip-markers
		--emit-index-url
		--emit-find-links
	)

	mkdir -p /tmp/ray-deps

	# Remove the GPU constraints
	cp python/requirements_compiled.txt /tmp/ray-deps/requirements_compiled.txt
	sed -i '/^--extra-index-url /d' /tmp/ray-deps/requirements_compiled.txt
	sed -i '/^--find-links /d' /tmp/ray-deps/requirements_compiled.txt

	# First, extract base test dependencies from the current compiled mono repo one.
	# This also expands to the indirect dependencies for this Python version & platform.
	#
	# Needs to use the exact torch version.
	echo "--- Compile ray base test dependencies"
	bazel run //ci/depsets:depsets -- compile -c "/tmp/ray-deps/requirements_compiled.txt" \
		-r "python/requirements.txt,python/requirements/cloud-requirements.txt,python/requirements/base-test-requirements.txt" \
		--header True ray_base_test_deps_${PYTHON_CUDA_CODE}

	# Second, expand it into LLM test dependencies
	echo "--- Expand ray base test dependencies into LLM test dependencies"
	bazel run //ci/depsets:depsets -- expand -s ray_base_test_deps -c "python/requirements_compiled_ray_test_${PYTHON_CUDA_CODE}.txt" \
		-r "python/requirements/cloud-requirements.txt,python/requirements/base-test-requirements.txt,python/requirements/llm/llm-requirements.txt,python/requirements/llm/llm-test-requirements.txt" \
		--header True ray_llm_test_deps_${PYTHON_CUDA_CODE}
	# Third, extract the ray base dependencies from ray base test dependencies.
	# TODO(aslonnie): This should be used for installing ray in the container images.
	echo "--- Compile ray base test dependencies"
	bazel run //ci/depsets:depsets -- compile -c ~/.depsets/ray_llm_test_deps_${PYTHON_CUDA_CODE} -r "python/requirements.txt" \
		--header True ray_compiled_deps_${PYTHON_CUDA_CODE}

	# Finally, extract the LLM dependencies from the LLM test dependencies,
	# which is also an expansion of the ray base dependencies.
	# TODO(aslonnie): This should be used for installing ray[llm] in the container images.
	echo "--- Compile LLM dependencies"
	bazel run //ci/depsets:depsets -- compile -c ~/.depsets/ray_llm_test_deps_${PYTHON_CUDA_CODE} -r "python/requirements.txt,python/requirements/llm/llm-requirements.txt" \
		--header True ray_llm_compiled_deps_${PYTHON_CUDA_CODE}
done

echo "--- Done"

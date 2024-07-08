#!/bin/bash

set -exuo pipefail

export RAY_INSTALL_JAVA="${RAY_INSTALL_JAVA:-0}"

# Python version key, interpreter version code
PYTHON_VERSIONS=(
  "py39 cp39-cp39"
  "py310 cp310-cp310"
  "py311 cp311-cp311"
  "py312 cp312-cp312"
)

# Add the repo folder to the safe.dictory global variable to avoid the failure
# because of secruity check from git, when executing the following command
# `git clean ...`,  while building wheel locally.
git config --global --add safe.directory /ray

# Setup runtime environment
./ci/build/build-manylinux-forge.sh
source "$HOME"/.nvm/nvm.sh

# Compile ray
./ci/build/build-manylinux-ray.sh

# Build ray wheel
for PYTHON_VERSIONS in "${PYTHON_VERSIONS[@]}" ; do
  PYTHON_VERSION_KEY="$(echo "${PYTHON_VERSIONS}" | cut -d' ' -f1)"
  if [[ "${BUILD_ONE_PYTHON_ONLY:-}" != "" && "${PYTHON_VERSION_KEY}" != "${BUILD_ONE_PYTHON_ONLY}" ]]; then
    continue
  fi

  PYTHON="$(echo "${PYTHON_VERSIONS}" | cut -d' ' -f2)"

  echo "--- Build wheel for ${PYTHON}"

  # The -f flag is passed twice to also run git clean in the arrow subdirectory.
  # The -d flag removes directories. The -x flag ignores the .gitignore file,
  # and the -e flag ensures that we don't remove the .whl directory, the
  # dashboard directory and jars directory, as well as the compiled
  # dependency constraints.
  git clean -f -f -x -d -e .whl -e python/ray/dashboard/client -e dashboard/client -e python/ray/jars -e python/requirements_compiled.txt

  ./ci/build/build-manylinux-wheel.sh "${PYTHON}"
done

# Clean the build output so later operations is on a clean directory.
git clean -f -f -x -d -e .whl -e python/ray/dashboard/client -e python/requirements_compiled.txt
bazel clean

# Build ray jar
./ci/build/build-manylinux-jar.sh

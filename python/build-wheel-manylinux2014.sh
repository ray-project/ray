#!/bin/bash

set -exuo pipefail

export RAY_INSTALL_JAVA="${RAY_INSTALL_JAVA:-0}"

# Python version key, interpreter version code, numpy tuples.
PYTHON_NUMPYS=(
  "py38 cp38-cp38 1.14.5"
  "py39 cp39-cp39 1.19.3"
  "py310 cp310-cp310 1.22.0"
  "py311 cp311-cp311 1.22.0"
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
IFS=',' read -r -a array <<< "$1"
for ((i=0; i<${#PYTHON_NUMPYS[@]}; ++i)); do
  PYTHON_NUMPY_TMP=${PYTHON_NUMPYS[i]}
  PYTHON="$(echo "${PYTHON_NUMPY_TMP}" | cut -d' ' -f2)"
  NUMPY_VERSION="$(echo "${PYTHON_NUMPY_TMP}" | cut -d' ' -f3)"

  find_in_input=false
  for element in "${array[@]}"; do
    if [ ${PYTHON} == $element ]; then
      find_in_input=true
      break 1
    fi
  done

  if [ $find_in_input == false ]; then
    continue
  fi

  PYTHON_VERSION_KEY="$(echo "${PYTHON_NUMPY_TMP}" | cut -d' ' -f1)"
  if [[ "${BUILD_ONE_PYTHON_ONLY:-}" != "" && "${PYTHON_VERSION_KEY}" != "${BUILD_ONE_PYTHON_ONLY}" ]]; then
    continue
  fi
  echo "--- Build wheel for ${PYTHON}, numpy=${NUMPY_VERSION}"

  # The -f flag is passed twice to also run git clean in the arrow subdirectory.
  # The -d flag removes directories. The -x flag ignores the .gitignore file,
  # and the -e flag ensures that we don't remove the .whl directory, the
  # dashboard directory and jars directory, as well as the compiled
  # dependency constraints.
  git clean -f -f -x -d -e .whl -e python/ray/dashboard/client -e dashboard/client -e python/ray/jars -e python/requirements_compiled.txt

  ./ci/build/build-manylinux-wheel.sh "${PYTHON}" "${NUMPY_VERSION}"
done

# Clean the build output so later operations is on a clean directory.
git clean -f -f -x -d -e .whl -e python/ray/dashboard/client -e python/requirements_compiled.txt
bazel clean

# Build ray jar
./ci/build/build-manylinux-jar.sh

#!/bin/bash

set -exuo pipefail

# Host user UID/GID
HOST_UID=${HOST_UID:-$(id -u)}
HOST_GID=${HOST_GID:-$(id -g)}

if [ "$EUID" -eq 0 ]; then

  # Install sudo
  yum -y install sudo

  # Create group and user
  groupadd -g "$HOST_GID" builduser
  useradd -m -u "$HOST_UID" -g "$HOST_GID" -d /ray builduser

  # Give sudo access
  echo "builduser ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

  exec sudo -E -u builduser HOME="$HOME" bash "$0" "$@"

  exit 0

fi

export RAY_INSTALL_JAVA="${RAY_INSTALL_JAVA:-0}"

# Python version key, interpreter version code
PYTHON_VERSIONS=(
  "py39 cp39-cp39"
  "py310 cp310-cp310"
  "py311 cp311-cp311"
  "py312 cp312-cp312"
  "py313 cp313-cp313"
)

# Setup runtime environment
./ci/build/build-manylinux-forge.sh

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

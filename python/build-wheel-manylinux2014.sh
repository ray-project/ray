#!/bin/bash

set -exuo pipefail

if [[ "$EUID" -eq 0 ]]; then

  HOME="/tmp"

  # Install sudo
  yum -y install sudo

  GROUP_NAME="builduser"
  declare -a UID_FLAG=()

  # --- Ensure group exists ---
  if ! getent group "$GROUP_NAME" >/dev/null; then
    groupadd "$GROUP_NAME"
  fi

  # --- Ensure user exists ---
  if ! id -u builduser >/dev/null 2>&1; then
    useradd -m -d "$HOME"/ -g "$GROUP_NAME" builduser
  fi

  mkdir -p "$HOME"/
  chown -R builduser:"$GROUP_NAME" "$HOME"/

  echo "builduser ALL=(ALL) NOPASSWD:ALL" > /etc/sudoers.d/builduser
  chmod 0440 /etc/sudoers.d/builduser

  exec sudo -E -u builduser HOME="$HOME" bash "$0" "$@" || {
    echo "Failed to exec as builduser." >&2
    exit 1
  }

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

# Extract prebuilt dashboard into expected location, only if it exists
if [[ -f /ray/dashboard_build.tar.gz ]]; then
  echo "Extracting /ray/dashboard_build.tar.gz..."
  mkdir -p /ray/python/ray/dashboard/client/build  # ensure target exists
  tar -xzf /ray/dashboard_build.tar.gz -C /ray/python/ray/dashboard/client/build
else
  echo "ERROR: /ray/dashboard_build.tar.gz not found. Aborting." >&2
  exit 1
fi

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
  git clean -f -f -x -d -e .whl -e python/ray/dashboard/client -e dashboard/client -e python/ray/jars -e python/requirements_compiled.txt -e dashboard_build.tar.gz

  ./ci/build/build-manylinux-wheel.sh "${PYTHON}"
done

# Clean the build output so later operations is on a clean directory.
git clean -f -f -x -d -e .whl -e python/ray/dashboard/client -e python/requirements_compiled.txt -e dashboard_build.tar.gz
bazel clean

# Build ray jar
./ci/build/build-manylinux-jar.sh

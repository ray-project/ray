#!/bin/bash
# Build Python-agnostic ray-cpp wheel.
#
# The ray-cpp wheel contains only C++ headers, libraries, and executables
# with no Python version-specific content. This script builds a single wheel
# tagged py3-none-manylinux2014_* that works with any Python 3 version.
#
# Usage: ./ci/build/build-cpp-wheel.sh
#
# Environment variables:
#   BUILDKITE_COMMIT - Git commit SHA for ray.__commit__ (required)

set -exuo pipefail

TRAVIS_COMMIT="${TRAVIS_COMMIT:-$BUILDKITE_COMMIT}"

# Use any Python 3 to run setuptools (wheel content is Python-agnostic)
PYTHON="cp310-cp310"

mkdir -p .whl
cd python

/opt/python/"${PYTHON}"/bin/pip install -q cython==3.0.12 setuptools==80.9.0

# Set the commit SHA in _version.py
if [[ -n "$TRAVIS_COMMIT" ]]; then
  sed -i.bak "s/{{RAY_COMMIT_SHA}}/$TRAVIS_COMMIT/g" ray/_version.py && rm ray/_version.py.bak
else
  echo "TRAVIS_COMMIT variable not set - required to populate ray.__commit__."
  exit 1
fi

# Determine platform tag based on architecture
ARCH=$(uname -m)
if [[ "$ARCH" == "x86_64" ]]; then
  PLAT_NAME="manylinux2014_x86_64"
elif [[ "$ARCH" == "aarch64" ]]; then
  PLAT_NAME="manylinux2014_aarch64"
else
  echo "ERROR: Unsupported architecture: $ARCH"
  exit 1
fi

echo "Building ray-cpp wheel (Python-agnostic, platform: $PLAT_NAME)..."

# Build wheel with py3-none tag (Python version-agnostic)
# SKIP_BAZEL_BUILD=1 because artifacts are pre-built from wanda cache
PATH="/opt/python/${PYTHON}/bin:$PATH" \
SKIP_BAZEL_BUILD=1 RAY_INSTALL_JAVA=0 RAY_INSTALL_CPP=1 \
"/opt/python/${PYTHON}/bin/python" setup.py bdist_wheel \
  --python-tag py3 \
  --plat-name "$PLAT_NAME"

mv dist/*.whl ../.whl/

#!/bin/bash
set -exuo pipefail

PYTHON="$1"
TRAVIS_COMMIT="${TRAVIS_COMMIT:-$BUILDKITE_COMMIT}"

# WHEEL_TYPE=cpp can be set to build only the ray-cpp wheel (skips ray wheel).
# Otherwise, the original default behavior is preserved for backwards compatibility:
#   - Always build ray wheel
#   - Also build ray-cpp wheel unless RAY_DISABLE_EXTRA_CPP=1

export RAY_BUILD_ENV="manylinux_py${PYTHON}"

mkdir -p .whl
cd python
/opt/python/"${PYTHON}"/bin/pip install -q cython==3.0.12 setuptools==80.9.0
# Set the commit SHA in _version.py.
if [[ -n "$TRAVIS_COMMIT" ]]; then
  sed -i.bak "s/{{RAY_COMMIT_SHA}}/$TRAVIS_COMMIT/g" ray/_version.py && rm ray/_version.py.bak
else
  echo "TRAVIS_COMMIT variable not set - required to populated ray.__commit__."
  exit 1
fi

# When building the wheel, we always set RAY_INSTALL_JAVA=0 because we
# have already built the Java code above.

export BAZEL_PATH="$HOME"/bin/bazel

# Pointing a default python3 symlink to the desired python version.
# This is required for building with bazel.
sudo ln -sf "/opt/python/${PYTHON}/bin/python3" /usr/local/bin/python3

# Build wheel(s)
if [[ "${WHEEL_TYPE:-}" == "cpp" ]]; then
  # cpp-only mode: build only ray-cpp wheel
  echo "Building ray-cpp wheel..."
  PATH="/opt/python/${PYTHON}/bin:$PATH" RAY_INSTALL_JAVA=0 \
  RAY_INSTALL_CPP=1 "/opt/python/${PYTHON}/bin/python" -m pip wheel -v -w dist . --no-deps
else
  # Default mode: build ray wheel, then cpp wheel unless disabled
  PATH="/opt/python/${PYTHON}/bin:$PATH" RAY_INSTALL_JAVA=0 \
  "/opt/python/${PYTHON}/bin/python" -m pip wheel -v -w dist . --no-deps

  if [[ "${RAY_DISABLE_EXTRA_CPP:-}" != 1 ]]; then
    PATH="/opt/python/${PYTHON}/bin:$PATH" RAY_INSTALL_JAVA=0 \
    RAY_INSTALL_CPP=1 "/opt/python/${PYTHON}/bin/python" -m pip wheel -v -w dist . --no-deps
  fi
fi

# Rename the wheels so that they can be uploaded to PyPI. TODO(rkn): This is a
# hack, we should use auditwheel instead.
for path in dist/*.whl; do
  if [[ -f "${path}" ]]; then
    out="${path//-linux/-manylinux2014}"
    if [[ "$out" != "$path" ]]; then
      mv "${path}" "${out}"
    fi
  fi
done
mv dist/*.whl ../.whl/

#!/bin/bash
set -exuo pipefail

PYTHON="$1"
TRAVIS_COMMIT="${TRAVIS_COMMIT:-$BUILDKITE_COMMIT}"

export RAY_BUILD_ENV="manylinux_py${PYTHON}"

mkdir -p .whl
cd python
# Pinned here and then actually used, because the wheel builds below pass
# --no-build-isolation. Without that flag pip ignores these and builds in an
# isolated environment, fetching setuptools, cython, wheel, pip and packaging
# unpinned -- five requests to files.pythonhosted.org per wheel build, resolving
# to whatever is newest that day, from a docker build that has no package mirror
# available to it.
# Not -q: these three are the only packages this script still fetches, and hiding
# the "Obtaining dependency information ... from https://files.pythonhosted.org/..."
# lines is how their exposure went uncounted in the first place.
/opt/python/"${PYTHON}"/bin/pip install cython==3.0.12 setuptools==80.9.0 wheel==0.45.1
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

# build ray wheel
PATH="/opt/python/${PYTHON}/bin:$PATH" RAY_INSTALL_JAVA=0 \
"/opt/python/${PYTHON}/bin/python" -m pip wheel -v -w dist . --no-deps --no-build-isolation


if [[ "${RAY_DISABLE_EXTRA_CPP:-}" != 1 ]]; then
  # build ray-cpp wheel
  PATH="/opt/python/${PYTHON}/bin:$PATH" RAY_INSTALL_JAVA=0 \
  RAY_INSTALL_CPP=1 "/opt/python/${PYTHON}/bin/python" -m pip wheel -v -w dist . --no-deps --no-build-isolation
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

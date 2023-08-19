#!/bin/bash
set -exuo pipefail

PYTHON="$1"
NUMPY_VERSION="$2"
TRAVIS_COMMIT="${TRAVIS_COMMIT:-$BUILDKITE_COMMIT}"

mkdir -p .whl
cd python
# Fix the numpy version because this will be the oldest numpy version we can
# support.
/opt/python/"${PYTHON}"/bin/pip install -q numpy=="${NUMPY_VERSION}" cython==0.29.32
# Set the commit SHA in __init__.py.
if [[ -n "$TRAVIS_COMMIT" ]]; then
  sed -i.bak "s/{{RAY_COMMIT_SHA}}/$TRAVIS_COMMIT/g" ray/__init__.py && rm ray/__init__.py.bak
else
  echo "TRAVIS_COMMIT variable not set - required to populated ray.__commit__."
  exit 1
fi

# When building the wheel, we always set RAY_INSTALL_JAVA=0 because we
# have already built the Java code above.

# build ray wheel
PATH="/opt/python/${PYTHON}/bin:$PATH" RAY_INSTALL_JAVA=0 \
"/opt/python/${PYTHON}/bin/python" setup.py -q bdist_wheel

# build ray-cpp wheel
PATH="/opt/python/${PYTHON}/bin:$PATH" RAY_INSTALL_JAVA=0 \
RAY_INSTALL_CPP=1 "/opt/python/${PYTHON}/bin/python" setup.py -q bdist_wheel

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

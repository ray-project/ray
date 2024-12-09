#!/bin/bash
set -exuo pipefail

PYTHON="$1"
TRAVIS_COMMIT="${TRAVIS_COMMIT:-$BUILDKITE_COMMIT}"

mkdir -p .whl
cd python
/opt/python/"${PYTHON}"/bin/pip install -q cython==0.29.37
# Set the commit SHA in _version.py.
if [[ -n "$TRAVIS_COMMIT" ]]; then
  sed -i.bak "s/{{RAY_COMMIT_SHA}}/$TRAVIS_COMMIT/g" ray/_version.py && rm ray/_version.py.bak
else
  echo "TRAVIS_COMMIT variable not set - required to populated ray.__commit__."
  exit 1
fi

# When building the wheel, we always set RAY_INSTALL_JAVA=0 because we
# have already built the Java code above.

# build ray wheel
PATH="/opt/python/${PYTHON}/bin:$PATH" RAY_INSTALL_JAVA=0 \
"/opt/python/${PYTHON}/bin/python" setup.py -q bdist_wheel


if [[ "${RAY_DISABLE_EXTRA_CPP:-}" != 1 ]]; then
  # build ray-cpp wheel
  PATH="/opt/python/${PYTHON}/bin:$PATH" RAY_INSTALL_JAVA=0 \
  RAY_INSTALL_CPP=1 "/opt/python/${PYTHON}/bin/python" setup.py -q bdist_wheel
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

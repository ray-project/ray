#!/bin/bash


set -euo pipefail

source "$HOME/.nvm/nvm.sh"

# Add the repo folder to the safe.dictory global variable to avoid the failure
# because of secruity check from git, when executing the following command
# `git clean ...`,  while building wheel locally.
git config --global --add safe.directory /ray

echo "--- Build dashboard frontend"

# Build the dashboard so its static assets can be included in the wheel.
# TODO(mfitton): switch this back when deleting old dashboard code.
(
  cd python/ray/dashboard/client
  npm ci
  npm run build
)

echo "--- Build python wheels"
mkdir -p .whl
# Python version key, interpreter version code, numpy tuples.
PYTHON_NUMPYS=(
  "py37 cp37-cp37m 1.14.5"
  "py38 cp38-cp38 1.14.5"
  "py39 cp39-cp39 1.19.3"
  "py310 cp310-cp310 1.22.0"
  "py311 cp311-cp311 1.22.0"
)
CYTHON_VERSION="0.29.32"

for PYTHON_NUMPY in "${PYTHON_NUMPYS[@]}" ; do
  PYTHON_VERSION_KEY="$(echo "${PYTHON_NUMPY}" | cut -d' ' -f1)"
  if [[ "${BUILD_ONE_PYTHON_ONLY:-}" != "" && "${PYTHON_VERSION_KEY}" != "${BUILD_ONE_PYTHON_ONLY}" ]]; then
    continue
  fi

  PYTHON="$(echo "${PYTHON_NUMPY}" | cut -d' ' -f2)"
  NUMPY_VERSION="$(echo "${PYTHON_NUMPY}" | cut -d' ' -f3)"

  echo "--- Build wheel for ${PYTHON}, numpy=${NUMPY_VERSION}"

  # The -f flag is passed twice to also run git clean in the arrow subdirectory.
  # The -d flag removes directories. The -x flag ignores the .gitignore file,
  # and the -e flag ensures that we don't remove the .whl directory, the
  # dashboard directory and jars directory.
  git clean -f -f -x -d -e .whl -e python/ray/dashboard/client -e dashboard/client -e python/ray/jars

  (
    cd python
    # Fix the numpy version because this will be the oldest numpy version we can
    # support.
    /opt/python/"${PYTHON}"/bin/pip install -q numpy=="${NUMPY_VERSION}" cython=="${CYTHON_VERSION}"
    # Set the commit SHA in __init__.py.
    if [[ -n "$TRAVIS_COMMIT" ]]; then
      sed -i.bak "s/{{RAY_COMMIT_SHA}}/$TRAVIS_COMMIT/g" ray/__init__.py && rm ray/__init__.py.bak
    else
      echo "TRAVIS_COMMIT variable not set - required to populated ray.__commit__."
      exit 1
    fi

    # build ray wheel
    PATH="/opt/python/${PYTHON}/bin:$PATH" \
    "/opt/python/${PYTHON}/bin/python" setup.py -q bdist_wheel

    # In the future, run auditwheel here.
    mv dist/*.whl ../.whl/
  )
done

# Rename the wheels so that they can be uploaded to PyPI. TODO(rkn): This is a
# hack, we should use auditwheel instead.
for path in .whl/*.whl; do
  if [[ -f "${path}" ]]; then
    out="${path//-linux/-manylinux2014}"
    if [[ "$out" != "$path" ]]; then
        mv "${path}" "${out}"
    fi
  fi
done

cp .whl/*.whl /artifacts/

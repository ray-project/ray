#!/bin/bash

# Cause the script to exit if a single command fails.
set -e

# Show explicitly which commands are currently running.
set -x

DOWNLOAD_DIR=python_downloads

NODE_VERSION="14"
PY_VERSIONS=("3.8.2"
             "3.9.1"
             "3.10.4")
PY_MMS=("3.8"
        "3.9"
        "3.10")


if [[ -n "${SKIP_DEP_RES}" ]]; then
  ./ci/env/install-bazel.sh

  curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.39.0/install.sh | bash
  curl -o- https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-MacOSX-arm64.sh | bash
  source ~/.bash_profile
  conda init bash
  source ~/.bash_profile

  # Use the latest version of Node.js in order to build the dashboard.
  source "$HOME"/.nvm/nvm.sh
  nvm install $NODE_VERSION
  nvm use $NODE_VERSION
fi

# Build the dashboard so its static assets can be included in the wheel.
pushd python/ray/dashboard/client
  npm ci
  npm run build
popd

mkdir -p .whl

for ((i=0; i<${#PY_VERSIONS[@]}; ++i)); do
  PY_MM=${PY_MMS[i]}
  CONDA_ENV_NAME="p$PY_MM"
 
  # The -f flag is passed twice to also run git clean in the arrow subdirectory.
  # The -d flag removes directories. The -x flag ignores the .gitignore file,
  # and the -e flag ensures that we don't remove the .whl directory.
  git clean -f -f -x -d -e .whl -e $DOWNLOAD_DIR -e python/ray/dashboard/client -e dashboard/client


  # Install python using conda. This should be easier to produce consistent results in buildkite and locally.
  source ~/.bash_profile
  conda create -y -n "$CONDA_ENV_NAME"
  conda activate "$CONDA_ENV_NAME"
  conda remove -y python || true
  conda install -y python="$PY_MM"

  # NOTE: We expect conda to set the PATH properly.
  PIP_CMD=pip
  PYTHON_EXE=python

  $PIP_CMD install --upgrade pip

  if [ -z "${TRAVIS_COMMIT}" ]; then
    TRAVIS_COMMIT=${BUILDKITE_COMMIT}
  fi

  pushd python
    # Setuptools on CentOS is too old to install arrow 0.9.0, therefore we upgrade.
    # TODO: Unpin after https://github.com/pypa/setuptools/issues/2849 is fixed.
    $PIP_CMD install --upgrade setuptools==58.4
    $PIP_CMD install -q cython==0.29.26
    # Install wheel to avoid the error "invalid command 'bdist_wheel'".
    $PIP_CMD install -q wheel
    # Set the commit SHA in __init__.py.
    if [ -n "$TRAVIS_COMMIT" ]; then
      echo "TRAVIS_COMMIT variable detected. ray.__commit__ will be set to $TRAVIS_COMMIT"
    else
      echo "TRAVIS_COMMIT variable is not set, getting the current commit from git."
      TRAVIS_COMMIT=$(git rev-parse HEAD)
    fi

    sed -i .bak "s/{{RAY_COMMIT_SHA}}/$TRAVIS_COMMIT/g" ray/__init__.py && rm ray/__init__.py.bak

    # Add the correct Python to the path and build the wheel. This is only
    # needed so that the installation finds the cython executable.
    # build ray wheel
    $PYTHON_EXE setup.py bdist_wheel
    # build ray-cpp wheel
    RAY_INSTALL_CPP=1 $PYTHON_EXE setup.py bdist_wheel
    mv dist/*.whl ../.whl/
  popd
done

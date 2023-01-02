#!/bin/bash

# Cause the script to exit if a single command fails.
set -e

# Show explicitly which commands are currently running.
set -x

# Much of this is taken from https://github.com/matthew-brett/multibuild.
# This script uses "sudo", so you may need to type in a password a couple times.

MACPYTHON_URL=https://www.python.org/ftp/python
MACPYTHON_PY_PREFIX=/Library/Frameworks/Python.framework/Versions
DOWNLOAD_DIR=python_downloads

NODE_VERSION="14"
PY_VERSIONS=("3.6.2"
             "3.7.0"
             "3.8.2"
             "3.9.1"
             "3.10.4"
             # "3.11.0"
             )
PY_INSTS=("python-3.6.2-macosx10.6.pkg"
          "python-3.7.0-macosx10.6.pkg"
          "python-3.8.2-macosx10.9.pkg"
          "python-3.9.1-macosx10.9.pkg"
          "python-3.10.4-macos11.pkg"
          # "python-3.11.0-macos11.pkg"
          )
PY_MMS=("3.6"
        "3.7"
        "3.8"
        "3.9"
        "3.10"
        # "3.11"
        )

NUMPY_VERSIONS=("1.14.5"
                "1.14.5"
                "1.14.5"
                "1.19.3"
                "1.22.0"
                # "1.24.0"
                )

./ci/env/install-bazel.sh

mkdir -p $DOWNLOAD_DIR
mkdir -p .whl

# Use the latest version of Node.js in order to build the dashboard.
source "$HOME"/.nvm/nvm.sh
nvm install $NODE_VERSION
nvm use $NODE_VERSION

# Build the dashboard so its static assets can be included in the wheel.
pushd python/ray/dashboard/client
  npm ci
  npm run build
popd

for ((i=0; i<${#PY_VERSIONS[@]}; ++i)); do
  PY_VERSION=${PY_VERSIONS[i]}
  PY_INST=${PY_INSTS[i]}
  PY_MM=${PY_MMS[i]}
  NUMPY_VERSION=${NUMPY_VERSIONS[i]}

  # The -f flag is passed twice to also run git clean in the arrow subdirectory.
  # The -d flag removes directories. The -x flag ignores the .gitignore file,
  # and the -e flag ensures that we don't remove the .whl directory.
  git clean -f -f -x -d -e .whl -e $DOWNLOAD_DIR -e python/ray/dashboard/client -e dashboard/client

  # Install Python.
  # In Buildkite, the Python packages are installed on the machien before the build has ran.
  PYTHON_EXE=$MACPYTHON_PY_PREFIX/$PY_MM/bin/python$PY_MM
  PIP_CMD="$(dirname "$PYTHON_EXE")/pip$PY_MM"

  if [ -z "${BUILDKITE}" ]; then
    INST_PATH=python_downloads/$PY_INST
    curl $MACPYTHON_URL/"$PY_VERSION"/"$PY_INST" > "$INST_PATH"
    sudo installer -pkg "$INST_PATH" -target /
    installer -pkg "$INST_PATH" -target /

    pushd /tmp
      # Install latest version of pip to avoid brownouts.
      curl https://bootstrap.pypa.io/get-pip.py | $PYTHON_EXE
    popd
  fi

  if [ -z "${TRAVIS_COMMIT}" ]; then
    TRAVIS_COMMIT=${BUILDKITE_COMMIT}
  fi

  pushd python
    # Setuptools on CentOS is too old to install arrow 0.9.0, therefore we upgrade.
    # TODO: Unpin after https://github.com/pypa/setuptools/issues/2849 is fixed.
    $PIP_CMD install --upgrade setuptools==58.4
    # Install setuptools_scm because otherwise when building the wheel for
    # Python 3.6, we see an error.
    $PIP_CMD install -q setuptools_scm==3.1.0
    # Fix the numpy version because this will be the oldest numpy version we can
    # support.
    $PIP_CMD install -q numpy=="$NUMPY_VERSION" cython==0.29.32
    # Install wheel to avoid the error "invalid command 'bdist_wheel'".
    $PIP_CMD install -q wheel
    # Set the commit SHA in __init__.py.
    if [ -n "$TRAVIS_COMMIT" ]; then
      sed -i.bak "s/{{RAY_COMMIT_SHA}}/$TRAVIS_COMMIT/g" ray/__init__.py && rm ray/__init__.py.bak
    else
      echo "TRAVIS_COMMIT variable not set - required to populated ray.__commit__."
      exit 1
    fi
    # Add the correct Python to the path and build the wheel. This is only
    # needed so that the installation finds the cython executable.
    # build ray wheel
    PATH=$MACPYTHON_PY_PREFIX/$PY_MM/bin:$PATH $PYTHON_EXE setup.py bdist_wheel
    # build ray-cpp wheel
    RAY_INSTALL_CPP=1 PATH=$MACPYTHON_PY_PREFIX/$PY_MM/bin:$PATH $PYTHON_EXE setup.py bdist_wheel
    mv dist/*.whl ../.whl/
  popd
done

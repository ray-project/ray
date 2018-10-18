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

PY_VERSIONS=("2.7.13"
             "3.4.4"
             "3.5.3"
             "3.6.1"
             "3.7.0")
PY_INSTS=("python-2.7.13-macosx10.6.pkg"
          "python-3.4.4-macosx10.6.pkg"
          "python-3.5.3-macosx10.6.pkg"
          "python-3.6.1-macosx10.6.pkg"
          "python-3.7.0-macosx10.6.pkg")
PY_MMS=("2.7"
        "3.4"
        "3.5"
        "3.6"
        "3.7")
# On python 3.7, a newer version of numpy seems to be necessary.
NUMPY_VERSIONS=("1.10.4"
                "1.10.4"
                "1.10.4"
                "1.14.5")

mkdir -p $DOWNLOAD_DIR
mkdir -p .whl

for ((i=0; i<${#PY_VERSIONS[@]}; ++i)); do
  PY_VERSION=${PY_VERSIONS[i]}
  PY_INST=${PY_INSTS[i]}
  PY_MM=${PY_MMS[i]}
  NUMPY_VERSION=${NUMPY_VERSIONS[i]}

  # The -f flag is passed twice to also run git clean in the arrow subdirectory.
  # The -d flag removes directories. The -x flag ignores the .gitignore file,
  # and the -e flag ensures that we don't remove the .whl directory.
  git clean -f -f -x -d -e .whl -e $DOWNLOAD_DIR

  # Install Python.
  INST_PATH=python_downloads/$PY_INST
  curl $MACPYTHON_URL/$PY_VERSION/$PY_INST > $INST_PATH
  sudo installer -pkg $INST_PATH -target /

  PYTHON_EXE=$MACPYTHON_PY_PREFIX/$PY_MM/bin/python$PY_MM
  PIP_CMD="$(dirname $PYTHON_EXE)/pip$PY_MM"

  pushd /tmp
    # Install latest version of pip to avoid brownouts
    curl https://bootstrap.pypa.io/get-pip.py | $PYTHON_EXE
  popd

  pushd python
    # Setuptools on CentOS is too old to install arrow 0.9.0, therefore we upgrade.
    $PIP_CMD install --upgrade setuptools
    # Install setuptools_scm because otherwise when building the wheel for
    # Python 3.6, we see an error.
    $PIP_CMD install -q setuptools_scm==2.1.0
    # Fix the numpy version because this will be the oldest numpy version we can
    # support.
    $PIP_CMD install -q numpy==$NUMPY_VERSION cython==0.27.3
    # Install wheel to avoid the error "invalid command 'bdist_wheel'".
    $PIP_CMD install -q wheel
    # Add the correct Python to the path and build the wheel. This is only
    # needed so that the installation finds the cython executable.
    INCLUDE_UI=1 PATH=$MACPYTHON_PY_PREFIX/$PY_MM/bin:$PATH $PYTHON_EXE setup.py bdist_wheel
    mv dist/*.whl ../.whl/
  popd
done

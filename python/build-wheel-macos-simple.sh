#!/bin/bash

# Much of this is taken from https://github.com/matthew-brett/multibuild.
# This script uses "sudo", so you may need to type in a password a couple times.

MACPYTHON_URL=https://www.python.org/ftp/python
MACPYTHON_PY_PREFIX=/Library/Frameworks/Python.framework/Versions
GET_PIP_URL=https://bootstrap.pypa.io/get-pip.py
DOWNLOAD_DIR=python_downloads

PY_VERSIONS=("2.7.13" "3.5.3")
PY_INSTS=("python-2.7.13-macosx10.6.pkg" "python-3.5.3-macosx10.6.pkg")
PY_MMS=("2.7" "3.5")

mkdir $DOWNLOAD_DIR
mkdir .whl

for ((i=0; i<${#PY_VERSIONS[@]}; ++i)); do
  PY_VERSION=${PY_VERSIONS[i]}
  PY_INST=${PY_INSTS[i]}
  PY_MM=${PY_MMS[i]}

  # The -f flag is passed twice to also run git clean in the arrow subdirectory.
  # The -d flag removes directories. The -x flag ignores the .gitignore file,
  # and the -e flag ensures that we don't remove the .whl directory.
  git clean -f -f -x -d -e .whl $DOWNLOAD_DIR

  # Install Python.
  curl $MACPYTHON_URL/$PY_VERSION/$PY_INST > python_downloads/$PY_INST
  sudo installer -pkg python_downloads/$PY_INST -target /
  PYTHON_EXE=$MACPYTHON_PY_PREFIX/$PY_MM/bin/python$PY_MM

  # Install pip. TODO(rkn): Is this necessary? Maybe pip is already installed.
  #curl $GET_PIP_URL > $DOWNLOAD_DIR/get-pip.py
  #sudo $PYTHON_EXE $DOWNLOAD_DIR/get-pip.py --ignore-installed
  PIP_CMD="$(dirname $PYTHON_EXE)/pip$PY_MM"

  pushd python
    $PYTHON_EXE setup.py bdist_wheel
    # Fix the numpy version because this will be the oldest numpy version we can
    # support.
    $PIP_CMD install numpy==1.10.4
    # Install wheel to avoid the error "invalid command 'bdist_wheel'".
    $PIP_CMD install wheel
    # Build the wheel.
    PATH=$MACPYTHON_PY_PREFIX/$PY_MM/bin:$PATH $PYTHON_EXE setup.py bdist_wheel
    # In the future, run auditwheel here.
    mv dist/*.whl ../.whl/
  popd
done

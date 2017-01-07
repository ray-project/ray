#!/usr/bin/env bash

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

echo "PYTHON is $PYTHON"

platform="unknown"
unamestr="$(uname)"
if [[ "$unamestr" == "Linux" ]]; then
  echo "Platform is linux."
  platform="linux"
elif [[ "$unamestr" == "Darwin" ]]; then
  echo "Platform is macosx."
  platform="macosx"
else
  echo "Unrecognized platform."
  exit 1
fi

if [[ "$PYTHON" == "2.7" ]] && [[ "$platform" == "linux" ]]; then
  sudo apt-get update
  sudo apt-get install -y cmake build-essential autoconf curl libtool python-dev python-numpy python-pip libboost-all-dev unzip
  sudo pip install funcsigs colorama psutil redis tensorflow
  sudo pip install --upgrade git+git://github.com/cloudpipe/cloudpickle.git@0d225a4695f1f65ae1cbb2e0bbc145e10167cce4
elif [[ "$PYTHON" == "3.5" ]] && [[ "$platform" == "linux" ]]; then
  sudo apt-get update
  sudo apt-get install -y cmake python-dev python-numpy build-essential autoconf curl libtool libboost-all-dev unzip
  # Install miniconda.
  wget https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh -O miniconda.sh
  bash miniconda.sh -b -p $HOME/miniconda
  export PATH="$HOME/miniconda/bin:$PATH"
  pip install numpy funcsigs colorama psutil redis tensorflow
  pip install --upgrade git+git://github.com/cloudpipe/cloudpickle.git@0d225a4695f1f65ae1cbb2e0bbc145e10167cce4
elif [[ "$PYTHON" == "2.7" ]] && [[ "$platform" == "macosx" ]]; then
  # check that brew is installed
  which -s brew
  if [[ $? != 0 ]]; then
    echo "Could not find brew, please install brew (see http://brew.sh/)."
    exit 1
  else
    echo "Updating brew."
    brew update
  fi
  brew install cmake automake autoconf libtool boost
  sudo easy_install pip
  sudo pip install numpy funcsigs colorama psutil redis tensorflow --ignore-installed six
  sudo pip install --upgrade git+git://github.com/cloudpipe/cloudpickle.git@0d225a4695f1f65ae1cbb2e0bbc145e10167cce4
elif [[ "$PYTHON" == "3.5" ]] && [[ "$platform" == "macosx" ]]; then
  # check that brew is installed
  which -s brew
  if [[ $? != 0 ]]; then
    echo "Could not find brew, please install brew (see http://brew.sh/)."
    exit 1
  else
    echo "Updating brew."
    brew update
  fi
  brew install cmake automake autoconf libtool boost
  # Install miniconda.
  wget https://repo.continuum.io/miniconda/Miniconda3-latest-MacOSX-x86_64.sh -O miniconda.sh
  bash miniconda.sh -b -p $HOME/miniconda
  export PATH="$HOME/miniconda/bin:$PATH"
  pip install numpy funcsigs colorama psutil redis tensorflow
  pip install --upgrade git+git://github.com/cloudpipe/cloudpickle.git@0d225a4695f1f65ae1cbb2e0bbc145e10167cce4
else
  echo "Unrecognized environment."
  exit 1
fi

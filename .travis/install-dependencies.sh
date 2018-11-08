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
  sudo apt-get install -y cmake pkg-config build-essential autoconf curl libtool python-dev python-numpy python-pip unzip
  # Install miniconda.
  wget https://repo.continuum.io/miniconda/Miniconda2-4.5.4-Linux-x86_64.sh -O miniconda.sh -nv
  bash miniconda.sh -b -p $HOME/miniconda
  export PATH="$HOME/miniconda/bin:$PATH"
  pip install -q cython==0.27.3 cmake tensorflow gym opencv-python pyyaml pandas==0.22 requests \
    feather-format lxml openpyxl xlrd py-spy setproctitle faulthandler
elif [[ "$PYTHON" == "3.5" ]] && [[ "$platform" == "linux" ]]; then
  sudo apt-get update
  sudo apt-get install -y cmake pkg-config python-dev python-numpy build-essential autoconf curl libtool unzip
  # Install miniconda.
  wget https://repo.continuum.io/miniconda/Miniconda3-4.5.4-Linux-x86_64.sh -O miniconda.sh -nv
  bash miniconda.sh -b -p $HOME/miniconda
  export PATH="$HOME/miniconda/bin:$PATH"
  pip install -q cython==0.27.3 cmake tensorflow gym opencv-python pyyaml pandas==0.22 requests \
    feather-format lxml openpyxl xlrd py-spy setproctitle
elif [[ "$PYTHON" == "2.7" ]] && [[ "$platform" == "macosx" ]]; then
  # check that brew is installed
  which -s brew
  if [[ $? != 0 ]]; then
    echo "Could not find brew, please install brew (see http://brew.sh/)."
    exit 1
  else
    echo "Updating brew."
    brew update > /dev/null
  fi
  brew install cmake pkg-config automake autoconf libtool openssl bison > /dev/null
  # Install miniconda.
  wget https://repo.continuum.io/miniconda/Miniconda2-4.5.4-MacOSX-x86_64.sh -O miniconda.sh -nv
  bash miniconda.sh -b -p $HOME/miniconda
  export PATH="$HOME/miniconda/bin:$PATH"
  pip install -q cython==0.27.3 cmake tensorflow gym opencv-python pyyaml pandas==0.22 requests \
    feather-format lxml openpyxl xlrd py-spy setproctitle faulthandler
elif [[ "$PYTHON" == "3.5" ]] && [[ "$platform" == "macosx" ]]; then
  # check that brew is installed
  which -s brew
  if [[ $? != 0 ]]; then
    echo "Could not find brew, please install brew (see http://brew.sh/)."
    exit 1
  else
    echo "Updating brew."
    brew update > /dev/null
  fi
  brew install cmake pkg-config automake autoconf libtool openssl bison > /dev/null
  # Install miniconda.
  wget https://repo.continuum.io/miniconda/Miniconda3-4.5.4-MacOSX-x86_64.sh -O miniconda.sh -nv
  bash miniconda.sh -b -p $HOME/miniconda
  export PATH="$HOME/miniconda/bin:$PATH"
  pip install -q cython==0.27.3 cmake tensorflow gym opencv-python pyyaml pandas==0.22 requests \
    feather-format lxml openpyxl xlrd py-spy setproctitle
elif [[ "$LINT" == "1" ]]; then
  sudo apt-get update
  sudo apt-get install -y cmake build-essential autoconf curl libtool unzip
  # Install miniconda.
  wget https://repo.continuum.io/miniconda/Miniconda3-4.5.4-Linux-x86_64.sh -O miniconda.sh -nv
  bash miniconda.sh -b -p $HOME/miniconda
  export PATH="$HOME/miniconda/bin:$PATH"
  # Install Python linting tools.
  pip install -q flake8 flake8-comprehensions
elif [[ "$LINUX_WHEELS" == "1" ]]; then
  sudo apt-get install docker
  sudo usermod -a -G docker travis
elif [[ "$MAC_WHEELS" == "1" ]]; then
  which -s brew
  if [[ $? != 0 ]]; then
    echo "Could not find brew, please install brew (see http://brew.sh/)."
    exit 1
  else
    echo "Updating brew."
    brew update > /dev/null
  fi
  brew install cmake pkg-config automake autoconf libtool openssl bison > /dev/null
  # We use true to avoid exiting with an error code because the brew install can
  # fail if a package is already installed.
  true
else
  echo "Unrecognized environment."
  exit 1
fi

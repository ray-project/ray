#!/usr/bin/env bash

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

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

if [[ $platform == "macosx" ]]; then
  # check that brew is installed
  which -s brew
  if [[ $? != 0 ]]; then
    echo "Could not find brew, please install brew (see http://brew.sh/)."
    exit 1
  else
    echo "Updating brew."
    brew update
  fi
fi

if [[ $platform == "linux" ]]; then
  # These commands must be kept in sync with the installation instructions.
  sudo apt-get update
  sudo apt-get install -y git cmake build-essential autoconf curl libtool python-dev python-numpy python-pip libboost-all-dev unzip graphviz
  sudo pip install ipython funcsigs subprocess32 protobuf colorama graphviz
  sudo pip install git+git://github.com/cloudpipe/cloudpickle.git@0d225a4695f1f65ae1cbb2e0bbc145e10167cce4  # We use the latest version of cloudpickle because it can serialize named tuples.
elif [[ $platform == "macosx" ]]; then
  # These commands must be kept in sync with the installation instructions.
  brew install git cmake automake autoconf libtool boost graphviz
  sudo easy_install pip
  sudo pip install ipython --user
  sudo pip install numpy funcsigs subprocess32 protobuf colorama graphviz --ignore-installed six
  sudo pip install git+git://github.com/cloudpipe/cloudpickle.git@0d225a4695f1f65ae1cbb2e0bbc145e10167cce4  # We use the latest version of cloudpickle because it can serialize named tuples.
fi

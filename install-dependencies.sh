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

if [[ $platform == "linux" ]]; then
  # These commands must be kept in sync with the installation instructions.
  sudo apt-get update
  sudo apt-get install -y cmake build-essential autoconf curl libtool python-dev python-numpy python-pip libboost-all-dev unzip nodejs npm
  sudo pip install funcsigs colorama psutil redis
elif [[ $platform == "macosx" ]]; then
  # check that brew is installed
  which -s brew
  if [[ $? != 0 ]]; then
    echo "Could not find brew, please install brew (see http://brew.sh/)."
    exit 1
  else
    echo "Updating brew."
    brew update
  fi
  # These commands must be kept in sync with the installation instructions.
  brew install cmake automake autoconf libtool boost node
  sudo easy_install pip
  sudo pip install numpy funcsigs colorama psutil redis --ignore-installed six
fi

sudo pip install --upgrade git+git://github.com/cloudpipe/cloudpickle.git@0d225a4695f1f65ae1cbb2e0bbc145e10167cce4  # We use the latest version of cloudpickle because it can serialize named tuples.
sudo pip install --upgrade --verbose git+git://github.com/ray-project/numbuf.git@488f881d708bc54e86ed375ee97aa94540808fa1

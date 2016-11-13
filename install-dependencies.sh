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
  sudo apt-get install -y cmake build-essential autoconf libtool python-dev python-numpy libboost-all-dev
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
  brew install cmake automake autoconf libtool boost
  sudo easy_install pip
  sudo pip install numpy --ignore-installed six
fi

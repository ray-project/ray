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
  sudo apt-get update
  sudo apt-get install -y git cmake build-essential autoconf curl libtool python-dev python-numpy python-pip libboost-all-dev unzip libjpeg8-dev graphviz
  sudo pip install -r requirements.txt
elif [[ $platform == "macosx" ]]; then
  brew install git cmake automake autoconf libtool boost libjpeg graphviz
  sudo easy_install pip
  sudo pip install numpy
  sudo pip install -r requirements.txt --ignore-installed six
fi
pushd "$ROOT_DIR/thirdparty"
  ./download_thirdparty.sh
  ./build_thirdparty.sh
popd

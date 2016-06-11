#!/bin/bash

platform="unknown"
unamestr=`uname`
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
  sudo apt-get install -y git cmake build-essential autoconf libtool python-dev python-numpy python-pip libboost-all-dev unzip libjpeg8-dev
elif [[ $platform == "macosx" ]]; then
  brew install git cmake autoconf libtool boost libjpeg
  sudo easy_install pip
  sudo pip install numpy
fi
sudo pip install -r requirements.txt
cd thirdparty
./download_thirdparty.sh
./build_thirdparty.sh
cd numbuf
cd python
sudo python setup.py install
mkdir -p ../../../build
cd ../../../build
cmake ..
make install
cd ../lib/python
sudo python setup.py install

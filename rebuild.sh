#!/bin/bash

if [ ! -f "src/computation_graph.cc" ]; then
  echo "Exiting this script because we may be in the wrong directory and we don't want to accidentally delete files."
  exit
fi

rm -rf build/*
pushd build
  cmake ..
  make install
popd

pushd lib/python
  sudo python setup.py install
popd

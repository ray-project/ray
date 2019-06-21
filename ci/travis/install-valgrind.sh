#!/usr/bin/env bash

# Cause the script to exit if a single command fails.
set -e

pushd ~

wget ftp://sourceware.org/pub/valgrind/valgrind-3.13.0.tar.bz2
tar xf valgrind-3.13.0.tar.bz2
cd valgrind-3.13.0
./autogen.sh
./configure
make -j8
sudo make install

popd

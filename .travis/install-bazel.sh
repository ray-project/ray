#!/usr/bin/env bash

# Cause the script to exit if a single command fails
set -e

OS=linux
ARCH=x86_64
if [[ "$TRAVIS_OS_NAME" == "osx" ]]; then OS=darwin; fi
URL="https://github.com/bazelbuild/bazel/releases/download/0.21.0/bazel-0.21.0-installer-${OS}-x86_64.sh"
wget -O install.sh $URL
chmod +x install.sh
./install.sh --user
rm -f install.sh

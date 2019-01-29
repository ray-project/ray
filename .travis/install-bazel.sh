#!/usr/bin/env bash

# Cause the script to exit if a single command fails
set -e

platform="unknown"
unamestr="$(uname)"
if [[ "$unamestr" == "Linux" ]]; then
  echo "Platform is linux."
  platform="linux"
elif [[ "$unamestr" == "Darwin" ]]; then
  echo "Platform is macosx."
  platform="darwin"
else
  echo "Unrecognized platform."
  exit 1
fi

URL="https://github.com/bazelbuild/bazel/releases/download/0.21.0/bazel-0.21.0-installer-${platform}-x86_64.sh"
wget -O install.sh $URL
chmod +x install.sh
./install.sh --user
rm -f install.sh

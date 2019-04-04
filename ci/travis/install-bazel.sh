#!/usr/bin/env bash

# Cause the script to exit if a single command fails
set -e

platform="unknown"
unamestr="$(uname)"
if [[ "$unamestr" == "Linux" ]]; then
  echo "Platform is Linux."
  platform="linux"
elif [[ "$unamestr" == "Darwin" ]]; then
  echo "Platform is macOS."
  platform="darwin"
elif [[ "$unamestr" == "MSYS_NT-10.0" ]]; then
  echo "Platform is Windows."
  platform="windows"
  ext="exe"
else
  echo "$unamestr is an unrecognized platform."
  exit 1
fi

URL="https://github.com/bazelbuild/bazel/releases/download/0.21.0/bazel-0.21.0-installer-${platform}-x86_64.${ext:-sh}"
wget -O install.sh $URL
chmod +x install.sh
./install.sh --user
rm -f install.sh

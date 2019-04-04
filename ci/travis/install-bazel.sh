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
  URL="https://github.com/bazelbuild/bazel/releases/download/0.21.0/bazel-0.21.0-windows-x86_64.exe"
  wget -O bazel_install.exe $URL
  ./bazel_install.exe
  exit 0
else
  echo "$unamestr is an unrecognized platform."
  exit 1
fi

URL="https://github.com/bazelbuild/bazel/releases/download/0.21.0/bazel-0.21.0-installer-${platform}-x86_64.sh"
wget -O install.sh $URL
chmod +x install.sh
./install.sh --user
rm -f install.sh

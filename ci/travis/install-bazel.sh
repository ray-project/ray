#!/usr/bin/env bash

# Cause the script to exit if a single command fails
set -e

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

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

URL="https://github.com/bazelbuild/bazel/releases/download/1.1.0/bazel-1.1.0-installer-${platform}-x86_64.sh"
wget -O install.sh $URL
chmod +x install.sh
./install.sh --user
rm -f install.sh

if [[ "$TRAVIS" == "true"  ]]; then
  # Use bazel disk cache if this script is running in Travis.
  mkdir -p $HOME/ray-bazel-cache
  echo "build --disk_cache=$HOME/ray-bazel-cache" >> $HOME/.bazelrc

  # Use ray google cloud cache
  echo "build --remote_cache=https://storage.googleapis.com/ray-bazel-cache" >> $HOME/.bazelrc
  if [[ "$TRAVIS_PULL_REQUEST" == "false" ]]; then
    # If we are in master build, we can write to the cache as well.
    openssl aes-256-cbc -K $encrypted_1c30b31fe1ee_key \
      -iv $encrypted_1c30b31fe1ee_iv \
      -in $ROOT_DIR/bazel_cache_credential.json.enc \
      -out $HOME/bazel_cache_credential.json -d
    echo "build --google_credentials=$HOME/bazel_cache_credential.json" >> $HOME/.bazelrc
  else
    echo "build --remote_upload_local_results=false" >> $HOME/.bazelrc
  fi
fi

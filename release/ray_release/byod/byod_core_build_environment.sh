#!/bin/bash
# This script is used to build an extra layer on top of the base anyscale/ray image
# to run RLlib release tests.

set -exo pipefail

# Dependencies required for building ray core
sudo apt-get update
sudo apt-get install -y build-essential curl clang-12 pkg-config psmisc unzip
curl -L -o bazel https://github.com/bazelbuild/bazelisk/releases/download/v1.26.0/bazelisk-linux-amd64
chmod +x bazel
sudo mv bazel /usr/local/bin
curl -L -o bazel-remote https://github.com/buchgr/bazel-remote/releases/download/v2.5.1/bazel-remote-2.5.1-linux-amd64
chmod +x bazel-remote
sudo mv bazel-remote /usr/local/bin

# Checkout the ray repo so we can build it
mkdir -p /tmp/ray-checkout-cache
git clone https://github.com/ray-project/ray.git /tmp/ray-checkout

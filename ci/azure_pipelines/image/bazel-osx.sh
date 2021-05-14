#!/bin/bash
################################################################################
##  File:  bazel.sh
##  Desc:  Installs Bazel
################################################################################

echo "Add Bazel distribution URI as a package source"
brew tap bazelbuild/tap
echo "Install and update Bazel"
brew install bazelbuild/tap/bazel

echo "Install Bazelisk"
brew install bazelisk

bazel --version

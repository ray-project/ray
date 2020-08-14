#!/bin/bash
################################################################################
##  File:  bazel.sh
##  Desc:  Installs Bazel
################################################################################

# Source the helpers for use with the script
source $HELPER_SCRIPTS/document.sh

echo "Add Bazel distribution URI as a package source"
sudo apt install curl
curl https://bazel.build/bazel-release.pub.gpg | sudo apt-key add -
echo "deb [arch=amd64] https://storage.googleapis.com/bazel-apt stable jdk1.8" | sudo tee /etc/apt/sources.list.d/bazel.list

echo "Install and update Bazel"
sudo apt update && sudo apt install bazel

echo "Install Bazelisk"
sudo npm install -g @bazel/bazelisk

DocumentInstalledItem "$(bazel version)"

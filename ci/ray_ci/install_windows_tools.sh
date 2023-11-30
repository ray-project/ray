#!/bin/bash

set -ex

# Install Bazel
powershell ci/pipeline/fix-windows-bazel.ps1

# Install Docker
mkdir -p /c/tools
wget -P /c/tools https://download.docker.com/win/static/stable/x86_64/docker-20.10.10.zip
unzip /c/tools/docker-20.10.10.zip -d /c/tools
rm /c/tools/docker-20.10.10.zip
mv /c/tools/docker/* /c/bazel/

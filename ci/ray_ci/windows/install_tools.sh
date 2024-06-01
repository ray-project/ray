#!/bin/bash

set -ex

# Delete the existing bazel and download bazelisk
powershell ci/ray_ci/windows/install_bazelisk.ps1

powershell ci/pipeline/fix-windows-container-networking.ps1
pip install awscli

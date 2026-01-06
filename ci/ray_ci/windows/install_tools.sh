#!/bin/bash

set -ex

# Delete the existing bazel and replace it with bazelisk.
powershell ci/ray_ci/windows/install_bazelisk.ps1

powershell ci/pipeline/fix-windows-container-networking.ps1
msiexec.exe /i https://awscli.amazonaws.com/AWSCLIV2.msi /qn

# syntax=docker/dockerfile:1.3-labs

FROM rayproject/buildenv:windows

ENV PYTHON=3.8
ENV RAY_USE_RANDOM_PORTS=1
ENV RAY_DEFAULT_BUILD=1
ENV LC_ALL=en_US.UTF-8
ENV LANG=en_US.UTF-8
ENV BUILD=1
ENV DL=1
ENV RAY_ENABLE_WINDOWS_OR_OSX_CLUSTER=1

RUN mkdir /rayci
WORKDIR /rayci
COPY . .

RUN <<EOF
#!/bin/bash

set -euo pipefail

git config --global core.symlinks true
git config --global core.autocrlf false
git init

powershell ci/pipeline/fix-windows-container-networking.ps1
powershell ci/pipeline/fix-windows-bazel.ps1

conda init
ci/ci.sh init
ci/ci.sh build

EOF

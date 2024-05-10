#!/bin/bash
set -exuo pipefail

TOOL_NAME="$1"
TOOL_BAZEL_TARGET="$2"
TOOL_BIN_PATH=${TOOL_BAZEL_TARGET/:/\/}

# build 
bazel clean
# the --incompatible_use_python_toolchains and --python_path are needed for compatibility with the hermetic_python rules
bazel build "$TOOL_BAZEL_TARGET" --build_python_zip --enable_runfiles --incompatible_use_python_toolchains=false --python_path=python
BAZEL_BIN="$(bazel info bazel-bin)"
INSTALLATION="$(mktemp -d)"
cp -L "${BAZEL_BIN}${TOOL_BIN_PATH}" "$INSTALLATION"/run

# release
ECR=830883877497.dkr.ecr.us-west-2.amazonaws.com

aws ecr get-login-password --region us-west-2 | docker login --username AWS --password-stdin "$ECR"
docker build -t "${RAYCI_WORK_REPO}:${RAYCI_BUILD_ID}-${TOOL_NAME}" -f anyscale/ci/rayci_tool.Dockerfile "$INSTALLATION"
docker push "${RAYCI_WORK_REPO}:${RAYCI_BUILD_ID}-${TOOL_NAME}"

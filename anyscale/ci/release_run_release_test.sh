#!/bin/bash
set -exuo pipefail

# build run_release_test
bazel clean
bazel build //release:run --build_python_zip --enable_runfiles
BAZEL_BIN=$(bazel info bazel-bin)
INSTALLATION=$(mktemp -d)
cp -L "$BAZEL_BIN"/release/run "$INSTALLATION"/run
cp -L "$BAZEL_BIN"/release/run_release_test "$INSTALLATION"/run_release_test

# build and release run_release_test docker image
ECR=830883877497.dkr.ecr.us-west-2.amazonaws.com

aws ecr get-login-password --region us-west-2 | docker login --username AWS --password-stdin "$ECR"
docker build --build-arg DOCKER_IMAGE_BASE_BUILD=python:3.9 -t "$RAYCI_WORK_REPO":"$RAYCI_BUILD_ID"-run_release_test -f anyscale/ci/release_run_release_test.Dockerfile "$INSTALLATION"
docker push "$RAYCI_WORK_REPO":"$RAYCI_BUILD_ID"-run_release_test

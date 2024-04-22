#!/bin/bash
set -exuo pipefail

# build run_release_test
bazel clean
# the --incompatible_use_python_toolchains and --python_path are needed for compatibility with the hermetic_python rules
bazel build //ci/ray_ci/pipeline:scheduler --build_python_zip --enable_runfiles --incompatible_use_python_toolchains=false --python_path=python
BAZEL_BIN="$(bazel info bazel-bin)"
INSTALLATION="$(mktemp -d)"
cp -L "$BAZEL_BIN"/ci/ray_ci/pipeline/scheduler "$INSTALLATION"/ci_pipeline_scheduler

# build and release run_release_test docker image
ECR=830883877497.dkr.ecr.us-west-2.amazonaws.com

aws ecr get-login-password --region us-west-2 | docker login --username AWS --password-stdin "$ECR"
docker build --build-arg DOCKER_IMAGE_BASE_BUILD=python:3.9 -t "${RAYCI_WORK_REPO}:${RAYCI_BUILD_ID}-ci_pipeline_scheduler" -f anyscale/ci/release_ci_pipeline_scheduler.Dockerfile "$INSTALLATION"
docker push "${RAYCI_WORK_REPO}:${RAYCI_BUILD_ID}-ci_pipeline_scheduler"

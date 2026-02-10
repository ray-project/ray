#!/bin/bash

set -euo pipefail

if [[ ${BUILDKITE_COMMIT} == "HEAD" ]]; then
  BUILDKITE_COMMIT="$(git rev-parse HEAD)"
  export BUILDKITE_COMMIT
fi

# Get build ID from environment variables
BUILD_ID="${RAYCI_BUILD_ID:-}"

if [[ -z "${BUILD_ID}" ]]; then
    if [[ -n "${BUILDKITE_BUILD_ID:-}" ]]; then
        # Generate SHA256 hash of BUILDKITE_BUILD_ID and take first 8 chars
        BUILD_ID=$(echo -n "${BUILDKITE_BUILD_ID}" | sha256sum | cut -c1-8)
    fi
fi

export RAYCI_BUILD_ID="${BUILD_ID}"
echo "RAYCI_BUILD_ID: ${RAYCI_BUILD_ID}"

aws ecr get-login-password --region us-west-2 | \
    docker login --username AWS --password-stdin 029272617770.dkr.ecr.us-west-2.amazonaws.com

bash release/gcloud_docker_login.sh release/aws2gce_iam.json
export PATH="${PWD}/google-cloud-sdk/bin:$PATH"


echo "--- Install Bazel"
curl -sSfLo /tmp/bazel https://github.com/bazelbuild/bazelisk/releases/download/v1.19.0/bazelisk-linux-amd64
chmod +x /tmp/bazel


echo "--- Install uv"

UV_PYTHON_VERSION=3.10
curl -LsSf https://astral.sh/uv/install.sh | sh
UV_BIN="${HOME}/.local/bin/uv"
"${UV_BIN}" python install "${UV_PYTHON_VERSION}"
UV_PYTHON_BIN="$("${UV_BIN}" python find --no-project "${UV_PYTHON_VERSION}")"


echo "--- Generate custom build steps"

if [[ "${AUTOMATIC:-0}" == "1" && "${BUILDKITE_BRANCH}" == "master" ]]; then
  export REPORT_TO_RAY_TEST_DB=1
fi

RUN_FLAGS=()
if [[ "${AUTOMATIC:-0}" == "0" || "${BUILDKITE_BRANCH}" == "releases/"* ]]; then
  RUN_FLAGS+=(--run-jailed-tests)
fi
if [[ "${BUILDKITE_BRANCH}" != "releases/"* ]]; then
  RUN_FLAGS+=(--run-unstable-tests)
fi

/tmp/bazel build --python_path="${UV_PYTHON_BIN}" \
  --build_python_zip --enable_runfiles \
  --incompatible_use_python_toolchains=false \
  //release:custom_image_build_and_test_init

BUILD_WORKSPACE_DIRECTORY="${PWD}" bazel-bin/release/custom_image_build_and_test_init \
  "${RUN_FLAGS[@]}" \
  --custom-build-jobs-output-file .buildkite/release/custom_build_jobs.rayci.yaml \
  --test-jobs-output-file .buildkite/release/release_tests.json

buildkite-agent pipeline upload .buildkite/release/release_tests.json

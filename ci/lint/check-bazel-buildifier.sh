#!/usr/bin/env bash
# Checks bazel buildifier format

set -euxo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE:-$0}")" || exit; pwd)/.."
LINT_BAZEL_TMP="$(mktemp -d)"
curl -sl "https://github.com/bazelbuild/buildtools/releases/download/v6.1.2/buildifier-linux-amd64" \
    -o "${LINT_BAZEL_TMP}/buildifier"
chmod +x "${LINT_BAZEL_TMP}/buildifier"
BUILDIFIER="${LINT_BAZEL_TMP}/buildifier" "${ROOT_DIR}/lint/bazel-format.sh"

rm -rf "${LINT_BAZEL_TMP}"  # Clean up

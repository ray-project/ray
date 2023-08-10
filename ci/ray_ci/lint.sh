#!/usr/bin/env bash

set -euxo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)/.."

annotations() {
  "${ROOT_DIR}"/lint/check_api_annotations.py
}

LINT=1 "${ROOT_DIR}"/env/install-dependencies.sh
"$@"

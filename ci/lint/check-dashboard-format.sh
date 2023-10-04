#!/usr/bin/env bash
# Checks front-end code format

set -euxo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE:-$0}")" || exit; pwd)/.."
WORKSPACE_DIR="${ROOT_DIR}/.."

cd "${WORKSPACE_DIR}"/python/ray/dashboard/client || exit

npm ci
FILENAMES=($(find src -name "*.ts" -or -name "*.tsx"))
node_modules/.bin/eslint --max-warnings 0 "${FILENAMES[@]}"
node_modules/.bin/prettier --check "${FILENAMES[@]}"
node_modules/.bin/prettier --check public/index.html

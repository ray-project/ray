#!/usr/bin/env bash
# Checks front-end code format

set -euxo pipefail

FILENAMES=("$@")
npm ci
FILENAMES=($(find src -name "*.ts" -or -name "*.tsx"))
node_modules/.bin/eslint --max-warnings 0 "${FILENAMES[@]}"

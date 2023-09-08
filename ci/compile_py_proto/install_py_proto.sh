#!/bin/bash

set -euo pipefail

mkdir -p "${BUILD_WORKSPACE_DIRECTORY}"/python/ray/core/generated
tar -xvf "$1" -C "${BUILD_WORKSPACE_DIRECTORY}"/python/ray/core/generated

mkdir -p "${BUILD_WORKSPACE_DIRECTORY}"/python/ray/serve/generated
tar -xvf "$2" -C "${BUILD_WORKSPACE_DIRECTORY}"/python/ray/serve/generated

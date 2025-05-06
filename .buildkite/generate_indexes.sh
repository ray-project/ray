#!/bin/bash

# Get prefix from args
PREFIX="${1}"

# Check if prefix is provided
if [[ -z "${PREFIX}" ]]; then
    echo "Error: prefix argument is required"
    echo "Usage: $0 <prefix>"
    exit 1
fi

bazel run //ci/ray_ci/automation:generate_index -- --prefix "${PREFIX}"

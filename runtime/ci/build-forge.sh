#!/bin/bash

set -euo pipefail

export DOCKER_BUILDKIT=1

echo "--- Build rayforge"

tar --mtime="UTC 2020-01-01" -c -f - \
	runtime/ci/rayforge/Dockerfile \
    | docker build --progress=plain -t anyscale/rayforge \
        -f runtime/ci/rayforge/Dockerfile -

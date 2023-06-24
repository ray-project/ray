#!/bin/bash

set -euxo pipefail

export DOCKER_BUILDKIT=1

tar --mtime="UTC 2020-01-01" -c -f - \
	runtime/ci/rayforge/Dockerfile \
    | docker build --progress=plain -t anyscale/rayforge \
        -f runtime/ci/rayforge/Dockerfile -

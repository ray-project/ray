#!/bin/bash

# build-forge.sh builds a container called anyscale/rayforge.  it is the based
# off a manylinux container but with the additional C++, Java and Javascript
# toolchain for building Ray C++, Java and Ray dashboard.

set -euo pipefail

export DOCKER_BUILDKIT=1

echo "--- Build rayforge"

tar --mtime="UTC 2020-01-01" -c -f - \
	runtime/ci/rayforge/Dockerfile \
    | docker build --progress=plain -t anyscale/rayforge \
        -f runtime/ci/rayforge/Dockerfile -

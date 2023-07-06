#!/bin/bash

# build-forge.sh builds a container called anyscale/rayforge.  it is the based
# off a manylinux container but with the additional C++, Java and Javascript
# toolchain for building Ray C++, Java and Ray dashboard.

set -euo pipefail

export DOCKER_BUILDKIT=1

echo "--- Build forge"

DEST_IMAGE="${CI_TMP_REPO}:${IMAGE_PREFIX}-forge"

tar --mtime="UTC 2020-01-01" -c -f - \
	anyscale/ci/rayforge/Dockerfile \
    | docker build --progress=plain -t "${DEST_IMAGE}" \
        -f anyscale/ci/rayforge/Dockerfile -

docker push "${DEST_IMAGE}"

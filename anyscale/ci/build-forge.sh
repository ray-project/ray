#!/bin/bash

# build-forge.sh builds a container called anyscale/rayforge.  it is the based
# off a manylinux container but with the additional C++, Java and Javascript
# toolchain for building Ray C++, Java and Ray dashboard.

set -euo pipefail

export DOCKER_BUILDKIT=1

echo "--- Build forge"

DEST_IMAGE="${CI_TMP_REPO}:${IMAGE_PREFIX}-forge-${IMAGE_SUFFIX}"

if [[ $(uname -m) == "x86_64" ]]; then
    BASE_IMAGE="quay.io/pypa/manylinux2014_x86_64:2022-12-20-b4884d9"
else
    BASE_IMAGE="quay.io/pypa/manylinux2014_aarch64:2022-12-20-b4884d9"
fi

tar --mtime="UTC 2020-01-01" -c -f - \
	anyscale/ci/rayforge/Dockerfile \
    | docker build --progress=plain -t "${DEST_IMAGE}" \
        --build-arg BAZEL_REMOTE_CACHE_URL \
        --build-arg BASE_IMAGE="${BASE_IMAGE}" \
        -f anyscale/ci/rayforge/Dockerfile -

docker push "${DEST_IMAGE}"

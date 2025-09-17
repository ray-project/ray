#!/bin/bash

set -euo pipefail

set -x

export RAYCI_WORK_REPO="localhost:5000/rayci-work"

wanda "ci/docker/base.test.py39.wanda.yaml"
wanda "ci/docker/base.build.py39.wanda.yaml"

IMAGE_FROM="cr.ray.io/rayproject/oss-ci-base_build" \
IMAGE_TO="corebuild" \
RAYCI_IS_GPU_BUILD="false" \
    wanda "ci/docker/core.build.py39.wanda.yaml"

bazelisk run //ci/ray_ci:test_in_docker -- //java/... core --build-only

docker run -ti --rm --shm-size=2.5gb \
    localhost:5000/rayci-work:corebuild \
    /bin/bash -iecuo pipefail "./java/test.sh"

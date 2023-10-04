#!/bin/bash
set -euo pipefail

SOURCE_IMAGE="$1"
DEST_IMAGE="$2"
REQUIREMENTS="$3"

DATAPLANE_S3_BUCKET="ray-release-automation-results"
DATAPLANE_FILENAME="dataplane_20230718.tgz"
DATAPLANE_DIGEST="a3ad426b05f5cf1981fe684ccbffc1dded5e1071a99184d1072b7fc7b4daf8bc"

# download dataplane build file
aws s3api get-object --bucket "${DATAPLANE_S3_BUCKET}" \
    --key "${DATAPLANE_FILENAME}" "${DATAPLANE_FILENAME}"

# check dataplane build file digest
echo "${DATAPLANE_DIGEST}  ${DATAPLANE_FILENAME}" | sha256sum -c

# build anyscale image
DOCKER_BUILDKIT=1 docker build \
    --build-arg BASE_IMAGE="$SOURCE_IMAGE" \
    -t "$DEST_IMAGE" - < "${DATAPLANE_FILENAME}"

DOCKER_BUILDKIT=1 docker build \
    --build-arg BASE_IMAGE="$DEST_IMAGE" \
    --build-arg PIP_REQUIREMENTS="$REQUIREMENTS" \
    --build-arg DEBIAN_REQUIREMENTS=requirements_debian_byod.txt \
    -t "$DEST_IMAGE" \
    -f release/ray_release/byod/byod.Dockerfile \
    release/ray_release/byod

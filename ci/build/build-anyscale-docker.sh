#!/bin/bash
set -euo pipefail

SOURCE_IMAGE="$1"
DEST_IMAGE="$2"
REQUIREMENTS="$3"
ECR="$4"

DATAPLANE_S3_BUCKET="ray-release-automation-results"
DATAPLANE_FILENAME="dataplane_20240613.tar.gz"
DATAPLANE_DIGEST="b7c68dd3cf9ef05b2b1518a32729fc036f06ae83bae9b9ecd241e33b37868944"

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

# publish anyscale image
aws ecr get-login-password --region us-west-2 | \
    docker login --username AWS --password-stdin "$ECR"
docker push "$DEST_IMAGE"

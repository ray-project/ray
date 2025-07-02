#!/bin/bash
set -exuo pipefail

# Host user UID/GID
HOST_UID=$(id -u)
HOST_GID=$(id -g)

IMAGE_NAME="ray-builder-amd64"

# Step 1: Build the Docker image
docker buildx build \
  --build-arg HOST_UID="$HOST_UID" \
  --build-arg HOST_GID="$HOST_GID" \
  -t $IMAGE_NAME - <<EOF
FROM quay.io/pypa/manylinux2014_x86_64:2024-07-02-9ac04ee

ARG HOST_UID
ARG HOST_GID

# Install sudo + bazel
RUN yum -y install sudo

# Create non-root user with matching UID/GID and sudo access
RUN groupadd -g \$HOST_GID builduser && \
    useradd -m -u \$HOST_UID -g \$HOST_GID -d /ray builduser && \
    echo "builduser ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers
EOF

# Step 2: Run the container as non-root user
docker run -ti --rm \
    --user "$HOST_UID:$HOST_GID" \
    -e BUILDKITE_COMMIT="$(git rev-parse HEAD)" \
    -e BUILD_ONE_PYTHON_ONLY=py39 \
    -w /ray -v "$(pwd)":/ray \
    -e HOME=/tmp \
    $IMAGE_NAME \
    /ray/python/build-wheel-manylinux2014.sh

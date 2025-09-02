#!/bin/bash
set -euo pipefail

SOURCE_IMAGE="$1"
DEST_IMAGE="$2"
ECR="$3"

# publish anyscale image
aws ecr get-login-password --region us-west-2 | \
    docker login --username AWS --password-stdin "$ECR"

docker tag "$SOURCE_IMAGE" "$DEST_IMAGE"
docker push "$DEST_IMAGE"

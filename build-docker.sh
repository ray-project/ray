#!/bin/bash
# This script is for users to build docker images locally. It is most useful for users wishing to edit the
# base-deps, ray-deps, or ray images. This script is *not* tested, so please look at the
# scripts/build-docker-images.py if there are problems with using this script.

set -ex

GPU=""
BASE_IMAGE=${BASE_IMAGE:-"ubuntu:focal"}
DOCKER_PREFIX=${DOCKER_PREFIX:-"rayproject/"}
PYTHON_VERSION=3.8.6

while [[ $# -gt 0 ]]
do
key="$1"
case $key in
    --gpu)
    GPU="-gpu"
    BASE_IMAGE="nvidia/cuda:11.2.0-cudnn8-devel-ubuntu18.04"
    ;;
    --no-cache-build)
    NO_CACHE="--no-cache"
    ;;
    --build-development-image)
    BUILD_DEV=YES
    ;;
    --build-examples)
    BUILD_EXAMPLES=YES
    ;;
    --shas-only)
    # output the SHA sum of each build. This is useful for scripting tests,
    # especially when builds of different versions are running on the same machine.
    # It also can facilitate cleanup.
    OUTPUT_SHA=YES
    ;;
    --wheel-to-use)
    # Which wheel to use. This defaults to the latest nightly on python 3.7
    echo "not implemented, just hardcode me :'("
    exit 1
    ;;
    --python-version)
    # Python version to install. e.g. 3.7.7.
    # Changing python versions may require a different wheel.
    # If not provided defaults to 3.7.7
    shift
    PYTHON_VERSION=$1
    ;;
    *)
    echo "Usage: build-docker.sh [ --no-cache-build ] [ --shas-only ] [ --build-development-image ] [ --build-examples ] [ --wheel-to-use ] [ --python-version ]"
    exit 1
esac
shift
done

# Build base-deps, ray-deps, and ray.
for IMAGE in "base-deps" "ray-deps" "ray-wheel" "ray" "ray-nb"
do
  rm ./docker/${IMAGE}/ray.tar ./docker/${IMAGE}/git-rev ./docker/${IMAGE}/requirements.txt || echo "No need to cleanup"
  git rev-parse HEAD > ./docker/${IMAGE}/git-rev
  git archive -o ./docker/${IMAGE}/ray.tar "$(git rev-parse HEAD)"
  cp ./python/requirements.txt ./docker/${IMAGE}/requirements.txt
    IMAGE_SHA=$(docker buildx build $NO_CACHE --build-arg GPU="$GPU" --build-arg BASE_IMAGE="$BASE_IMAGE" --build-arg PYTHON_VERSION="$PYTHON_VERSION" --build-arg DOCKER_PREFIX="$DOCKER_PREFIX"  -t ${DOCKER_PREFIX}$IMAGE:nightly$GPU --platform linux/arm64,linux/amd64 --push docker/$IMAGE )
    if [ $OUTPUT_SHA ]; then
	echo "rayproject/$IMAGE:nightly$GPU SHA:$IMAGE_SHA"
    fi
    rm ./docker/${IMAGE}/ray.tar ./docker/${IMAGE}/git-rev ./docker/${IMAGE}/requirements.txt
done


if [ $BUILD_EXAMPLES ]; then
    if [ $OUTPUT_SHA ]; then
	IMAGE_SHA=$(docker build $NO_CACHE -q -t rayproject/examples docker/examples)
	echo "rayproject/examples:latest SHA:$IMAGE_SHA"
    else
	docker buildx build $NO_CACHE -t ${DOCKER_PREFIX}examples --platform linux/arm64,linux/amd64 --push docker/examples
    fi
fi

rm -rf "$WHEEL_DIR"

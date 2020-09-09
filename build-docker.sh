#!/bin/bash
# This script is for users to build docker images locally. It is most useful for users wishing to edit the
# base-deps, ray-deps, or ray images. This script is *not* tested, so please look at the 
# scripts/build-docker-images.sh if there are problems with using this script.

set -x

GPU=""
BASE_IMAGE="ubuntu:focal"
WHEEL_URL="https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-1.0.0-cp37-cp37m-manylinux1_x86_64.whl"

while [[ $# -gt 0 ]]
do
key="$1"
case $key in
    --gpu)
    GPU="-gpu"
    BASE_IMAGE="nvidia/cuda:11.0-cudnn8-runtime-ubuntu18.04"
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
    *)
    echo "Usage: build-docker.sh [ --no-cache-build ] [ --shas-only ] [ --build-development-image ] [ --build-examples ] [ --wheel-to-use ]"
    exit 1
esac
shift
done

WHEEL_DIR=$(mktemp -d)
wget --quiet "$WHEEL_URL" -P "$WHEEL_DIR"
WHEEL="$WHEEL_DIR/$(basename "$WHEEL_DIR"/*.whl)"
# Build base-deps, ray-deps, and ray.
for IMAGE in "base-deps" "ray-deps" "ray"
do
    cp "$WHEEL" "docker/$IMAGE/$(basename "$WHEEL")"
    if [ $OUTPUT_SHA ]; then
        IMAGE_SHA=$(docker build $NO_CACHE --build-arg GPU="$GPU" --build-arg BASE_IMAGE="$BASE_IMAGE" --build-arg WHEEL_PATH="$(basename "$WHEEL")" -q -t rayproject/$IMAGE:latest docker/$IMAGE)
        echo "rayproject/$IMAGE:latest SHA:$IMAGE_SHA"
    else
        docker build $NO_CACHE  --build-arg GPU="$GPU" --build-arg BASE_IMAGE="$BASE_IMAGE" --build-arg WHEEL_PATH="$(basename "$WHEEL")" -t rayproject/$IMAGE:latest docker/$IMAGE
    fi
    rm "docker/$IMAGE/$(basename "$WHEEL")"
done 


# Build the current Ray source
if [ $BUILD_DEV ]; then 
    git rev-parse HEAD > ./docker/development/git-rev
    git archive -o ./docker/development/ray.tar "$(git rev-parse HEAD)"
    if [ $OUTPUT_SHA ]; then
        IMAGE_SHA=$(docker build --no-cache -q -t rayproject/development docker/development)
        echo "rayproject/development:latest SHA:$IMAGE_SHA"
    else
        docker build --no-cache -t rayproject/development docker/development
    fi
    rm ./docker/development/ray.tar ./docker/development/git-rev
fi

if [ $BUILD_EXAMPLES ]; then 
    if [ $OUTPUT_SHA ]; then
        IMAGE_SHA=$(docker build $NO_CACHE -q -t rayproject/examples docker/examples)
        echo "rayproject/examples:latest SHA:$IMAGE_SHA"
    else
        docker build $NO_CACHE -t rayproject/examples docker/examples
    fi
fi

rm -rf "$WHEEL_DIR"
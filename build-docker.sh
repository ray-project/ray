#!/bin/bash
# This script is for users to build docker images locally. It is most useful for users wishing to edit the
# base-deps, ray-deps, or ray images. This script is *not* tested, so please look at the 
# scripts/build-docker-images.py if there are problems with using this script.

set -x

GPU=""
BASE_IMAGE="ubuntu:focal"
WHEEL_URL="https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-2.0.0.dev0-cp37-cp37m-manylinux2014_x86_64.whl"
PYTHON_VERSION="3.7.7"


while [[ $# -gt 0 ]]
do
key="$1"
case $key in
    --gpu)
    GPU="-gpu"
    BASE_IMAGE="nvidia/cuda:11.2.0-cudnn8-devel-ubuntu18.04"
    ;;
    --base-image)
    # Override for the base image.
    shift
    BASE_IMAGE=$1
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
    echo "Usage: build-docker.sh [ --gpu ] [ --base-image ] [ --no-cache-build ] [ --shas-only ] [ --build-development-image ] [ --build-examples ] [ --wheel-to-use ] [ --python-version ]"
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
        IMAGE_SHA=$(docker build $NO_CACHE --build-arg GPU="$GPU" --build-arg BASE_IMAGE="$BASE_IMAGE" --build-arg WHEEL_PATH="$(basename "$WHEEL")" --build-arg PYTHON_VERSION="$PYTHON_VERSION" -q -t rayproject/$IMAGE:nightly$GPU docker/$IMAGE)
        echo "rayproject/$IMAGE:nightly$GPU SHA:$IMAGE_SHA"
    else
        docker build $NO_CACHE  --build-arg GPU="$GPU" --build-arg BASE_IMAGE="$BASE_IMAGE" --build-arg WHEEL_PATH="$(basename "$WHEEL")" --build-arg PYTHON_VERSION="$PYTHON_VERSION" -t rayproject/$IMAGE:nightly$GPU docker/$IMAGE
    fi
    rm "docker/$IMAGE/$(basename "$WHEEL")"
done 


# Build the current Ray source
if [ $BUILD_DEV ]; then 
    git rev-parse HEAD > ./docker/development/git-rev
    git archive -o ./docker/development/ray.tar "$(git rev-parse HEAD)"
    if [ $OUTPUT_SHA ]; then
        IMAGE_SHA=$(docker build $NO_CACHE -q -t rayproject/development docker/development)
        echo "rayproject/development:latest SHA:$IMAGE_SHA"
    else
        docker build $NO_CACHE -t rayproject/development docker/development
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

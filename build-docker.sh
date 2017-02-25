#!/bin/bash

while [[ $# -gt 0 ]]
do
key="$1"
case $key in
    --no-cache)
    NO_CACHE="--no-cache"
    ;;
    --skip-examples)
    SKIP_EXAMPLES=YES
    ;;
    *)
    echo "Usage: build-docker.sh [ --no-cache ] [ --skip-examples ]"
    exit 1
esac
shift
done

# Build base dependencies, allow caching
docker build -t $NO_CACHE ray-project/base-deps docker/base-deps

# Build the current Ray source
git rev-parse HEAD > ./docker/deploy/git-rev
git archive -o ./docker/deploy/ray.tar $(git rev-parse HEAD)
docker build --no-cache -t ray-project/deploy docker/deploy
rm ./docker/deploy/ray.tar ./docker/deploy/git-rev


if [ ! $SKIP_EXAMPLES ]; then
    docker build -t $NO_CACHE ray-project/examples docker/examples
fi

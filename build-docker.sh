#!/bin/bash
set -x

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
    --output-sha)
    # output the SHA sum of the last built file (either ray-project/deploy
    # or ray-project/examples, suppressing all other output. This is useful
    # for scripting tests, especially when builds of different versions
    # are running on the same machine. It also can facilitate cleanup.
    OUTPUT_SHA=YES
    ;;
    *)
    echo "Usage: build-docker.sh [ --no-cache ] [ --skip-examples ] [ --sha-sums ]"
    exit 1
esac
shift
done

# Build base dependencies, allow caching
if [ $OUTPUT_SHA ]; then
    IMAGE_SHA=$(docker build $NO_CACHE -q -t ray-project/base-deps docker/base-deps)
else
    docker build $NO_CACHE -t ray-project/base-deps docker/base-deps
fi

# Build the current Ray source
git rev-parse HEAD > ./docker/deploy/git-rev
git archive -o ./docker/deploy/ray.tar "$(git rev-parse HEAD)"
if [ $OUTPUT_SHA ]; then
    IMAGE_SHA=$(docker build --no-cache -q -t ray-project/deploy docker/deploy)
else
    docker build --no-cache -t ray-project/deploy docker/deploy
fi
rm ./docker/deploy/ray.tar ./docker/deploy/git-rev

# Build the examples, unless skipped
if [ ! $SKIP_EXAMPLES ]; then
    if [ $OUTPUT_SHA ]; then
        IMAGE_SHA=$(docker build $NO_CACHE -q -t ray-project/examples docker/examples)
    else
        docker build --no-cache -t ray-project/examples docker/examples
    fi
fi

if [ $OUTPUT_SHA ]; then
    echo "${IMAGE_SHA/sha256:/}"
fi

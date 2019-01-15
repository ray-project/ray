#!/usr/bin/env bash

# Cause the script to exit if a single command fails.
set -e

# Show explicitly which commands are currently running.
set -x

MEMORY_SIZE="20G"
SHM_SIZE="20G"

docker build --no-cache -t ray-project/base-deps docker/base-deps

# Add Ray source
git rev-parse HEAD > ./docker/deploy/git-rev
git archive -o ./docker/deploy/ray.tar $(git rev-parse HEAD)
DOCKER_SHA=$(docker build --no-cache -t ray-project/basic docker/basic)

echo "Using Docker image" $DOCKER_SHA

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} \
    -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY $DOCKER_SHA \
    bash /ray/test/stress_tests/run_stress_tests.sh


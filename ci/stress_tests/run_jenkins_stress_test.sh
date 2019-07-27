#!/usr/bin/env bash

# Cause the script to exit if a single command fails.
set -e

# Show explicitly which commands are currently running.
set -x

MEMORY_SIZE="20G"
SHM_SIZE="20G"

docker build -q --no-cache -t ray-project/base-deps docker/base-deps

# Add Ray source
git rev-parse HEAD > ./docker/stress_test/git-rev
git archive -o ./docker/stress_test/ray.tar $(git rev-parse HEAD)
DOCKER_SHA=$(docker build --no-cache -q -t ray-project/stress_test docker/stress_test)

echo "Using Docker image" $DOCKER_SHA
docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} \
    -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e RAY_AWS_SSH_KEY \
    $DOCKER_SHA \
    bash /ray/ci/stress_tests/run_stress_tests.sh

# docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} \
#     -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e RAY_AWS_SSH_KEY \
#     $DOCKER_SHA \
#     bash /ray/ci/stress_tests/run_application_stress_tests.sh

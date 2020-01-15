#!/bin/bash
#
# This script is for use in the Travis CI testing system. When running
# on Linux this script runs the command in a Docker container.
# Otherwise it runs the commands natively, which is what we want for
# the Mac OS X tests.
#
# Usage:
#
#   run_test.sh [OPTIONS] [COMMAND]
#
# Key options are:
#
#  --docker-image=img    Docker image to run the tests
#  --shm-size=xxx        Shared memory setting for Docker run command
#  --docker-only         Skip this test unless running on docker
#
# Example:
#
#   run_test.sh --docker-only --shm-size=500m \
#     --docker-image=ray-project/ray:test-examples \
#     'source setup-env.sh && cd examples/lbfgs && python driver.py'
#
# For further examples see this project's .travis.yml
#

# Argument parsing adapted from http://stackoverflow.com/a/14203146
for i in "$@"
do
TEST_COMMAND=''
case $i in
    --docker-image=*)
    DOCKER_IMAGE="${i#*=}"
    shift
    ;;
    --shm-size=*)
    SHM_SIZE="${i#*=}"
    shift
    ;;
    --docker-only)
    DOCKER_ONLY=YES
    ;;
    *)
    TEST_COMMAND=$i
    ;;
esac
done

if [[ $TRAVIS_OS_NAME == 'linux' ]]; then
    # Linux test uses Docker
    SHM_ARG=$([[ -z $SHM_SIZE ]] && echo "" || echo "--shm-size $SHM_SIZE")
    docker run $SHM_ARG $DOCKER_IMAGE bash -c "$TEST_COMMAND"
else
    # Mac OS X test
    if [[ -z $DOCKER_ONLY || $DOCKER_ONLY -ne YES ]]; then
        bash -c "$TEST_COMMAND"
    else
        echo "not on Docker, skipping test"
    fi
fi

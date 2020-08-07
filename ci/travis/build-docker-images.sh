#!/usr/bin/env bash

# This script build docker images for autoscaler.
# For now, we only build python3.6 images.
set -e
set -x

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)
ROOT_DIR=$(cd "$SCRIPT_DIR"/../../; pwd)
DOCKER_USERNAME="raytravisbot"

docker_push() {
    if [[ "$TRAVIS_PULL_REQUEST" == "false" ]]; then
        docker push "$@"
    else
        echo "Skipping docker push because it's in PR environment."
    fi
}

build_or_pull_base_deps() {
    docker pull rayproject/base-deps:latest
    age=`docker inspect -f '{{ .Created }}' rayproject/base-deps:latest`
    tag=`date +%F_%H`
    # Build if older than 1 week or updated in this PR
    if [[  `date -d "$date -7 days" +%F` > `date -d "$age" +%F` || "$RAY_CI_DOCKER_AFFECTED" == "1" ]]; then
        docker image rm rayproject/base-deps:latest
        docker build -t rayproject/base-deps docker/base-deps
    else
        echo "Just pulling an image"
    fi

    docker tag rayproject/base-deps rayproject/base-deps:"$tag"
    docker tag rayproject/base-deps rayproject/base-deps:latest
    docker_push rayproject/base-deps:"$tag"
    docker_push rayproject/base-deps:latest
}

# We will only build and push when we are building branch build.
if [[ "$TRAVIS" == "true" ]]; then

    if [[ "$TRAVIS_PULL_REQUEST" == "false" ]]; then
        echo "$DOCKER_PASSWORD" | docker login -u "$DOCKER_USERNAME" --password-stdin
    else
        if [[ "$RAY_CI_DOCKER_AFFECTED" == "0" ]]; then
            echo "Skipping docker build in PR build because dockerfile didn't change."
            exit 0
        fi
    fi

    wheel="$(basename "$ROOT_DIR"/.whl/*cp37m-manylinux*)"
    commit_sha=$(echo "$TRAVIS_COMMIT" | head -c 6)
    cp -r "$ROOT_DIR"/.whl "$ROOT_DIR"/docker/ray/.whl
    cp "$ROOT_DIR"/python/requirements.txt "$ROOT_DIR"/docker/autoscaler/requirements.txt
    cp "$ROOT_DIR"/python/requirements_autoscaler.txt "$ROOT_DIR"/docker/autoscaler/requirements_autoscaler.txt

    build_or_pull_base_deps()

    docker build \
        --build-arg WHEEL_PATH=".whl/$wheel" \
        -t rayproject/ray \
        "$ROOT_DIR"/docker/ray

    docker build \
        -t rayproject/autoscaler:"$commit_sha" \
        "$ROOT_DIR"/docker/autoscaler
 
    docker tag rayproject/ray rayproject/ray:"$commit_sha" 
    docker_push rayproject/ray:"$commit_sha"
    docker_push rayproject/autoscaler:"$commit_sha"


    # We have a branch build, e.g. release/v0.7.0
    if [[ "$TRAVIS_BRANCH" != "master" ]]; then
       # Replace / in branch name to - so it is legal tag name
       normalized_branch_name=$(echo "$TRAVIS_BRANCH" | sed -e "s/\//-/")
       docker tag rayproject/autoscaler:"$commit_sha" rayproject/autoscaler:"$normalized_branch_name"
       docker tag rayproject/ray:"$commit_sha" rayproject/ray:"$normalized_branch_name"
       docker_push rayproject/autoscaler:"$normalized_branch_name"
       docker_push rayproject/ray:"$normalized_branch_name"
    else
       docker tag rayproject/autoscaler:"$commit_sha" rayproject/autoscaler:latest
       docker tag rayproject/ray:"$commit_sha" rayproject/ray:latest
       docker_push rayproject/autoscaler:latest
       docker_push rayproject/ray:latest
    fi
fi


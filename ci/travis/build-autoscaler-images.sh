# This script build docker images for autoscaler.
# For now, we only build python3.6 images.
set -e
set -x

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)
ROOT_DIR=$(cd $SCRIPT_DIR/../../; pwd)
DOCKER_USERNAME="raytravisbot"

# We will only build and push when we are building branch build.
if [[ "$TRAVIS" == "true" && "$TRAVIS_PULL_REQUEST" == "false" ]]; then
    echo "$DOCKER_PASSWORD" | docker login -u "$DOCKER_USERNAME" --password-stdin

    docker build -q -t rayproject/base-deps docker/base-deps

    wheel="ray-0.8.0.dev7-cp36-cp36m-manylinux1_x86_64.whl"
    commit_sha=$(echo $TRAVIS_COMMIT | head -c 6)
    cp -r $ROOT_DIR/.whl $ROOT_DIR/docker/autoscaler/.whl

    docker build \
        --build-arg WHEEL_PATH=".whl/$wheel" \
        --build-arg WHEEL_NAME=$wheel \
        -t rayproject/autoscaler:$commit_sha \
        $ROOT_DIR/docker/autoscaler
    docker push rayproject/autoscaler:$commit_sha

    # We have a branch build, e.g. release/v0.7.0
    if [[ "$TRAVIS_BRANCH" != "master" ]]; then
       docker tag rayproject/autoscaler:$commit_sha rayproject/autoscaler:$TRAVIS_BRANCH
       docker push rayproject/autoscaler:$TRAVIS_BRANCH
    else
       docker tag rayproject/autoscaler:$commit_sha rayproject/autoscaler:latest
       docker push rayproject/autoscaler:latest
    fi
fi


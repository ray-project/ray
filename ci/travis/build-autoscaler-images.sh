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

    wheel=$(cd $ROOT_DIR/.whl; ls | grep cp36m-manylinux)
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
       # Replace / in branch name to - so it is legal tag name
       normalized_branch_name=$(echo $TRAVIS_BRANCH | sed -e "s/\//-/")
       docker tag rayproject/autoscaler:$commit_sha rayproject/autoscaler:$normalized_branch_name
       docker push rayproject/autoscaler:$normalized_branch_name
    else
       docker tag rayproject/autoscaler:$commit_sha rayproject/autoscaler:latest
       docker push rayproject/autoscaler:latest
    fi
fi


#!/bin/bash
# This script is not for normal use and is used in the event that CI (or a user) overwrites the latest tag.
set -x

IMAGE="1.6.0"
DEST="latest"

while [[ $# -gt 0 ]]
do
key="$1"
case $key in
    --source-tag)
    shift
    IMAGE=$1
    ;;
    --dest-tag)
    shift
    DEST=$1
    ;;
    *)
    echo "Usage: fix-docker-latest.sh --source-tag <TAG> --dest-tag <LATEST>"
    exit 1
esac
shift
done

echo "You must be logged into a user with push privileges to do this."
# for REPO in "ray" "ray-ml" "autoscaler" "ray-deps" "base-deps"
for REPO in "ray" "ray-ml" "autoscaler"
do
    for PYVERSION in "py36" "py37" "py38" "py39"
    do
        export SOURCE_TAG="$IMAGE"-"$PYVERSION"
        export DEST_TAG="$DEST"-"$PYVERSION"
        docker pull rayproject/"$REPO":"$SOURCE_TAG"
        docker tag rayproject/"$REPO":"$SOURCE_TAG" rayproject/"$REPO":"$DEST_TAG"
        docker tag rayproject/"$REPO":"$SOURCE_TAG" rayproject/"$REPO":"$DEST_TAG"-cpu

        docker pull rayproject/"$REPO":"$SOURCE_TAG"-gpu
        docker tag rayproject/"$REPO":"$SOURCE_TAG"-gpu rayproject/"$REPO":"$DEST_TAG"-gpu

        docker push rayproject/"$REPO":"$DEST_TAG"
        docker push rayproject/"$REPO":"$DEST_TAG"-cpu
        docker push rayproject/"$REPO":"$DEST_TAG"-gpu
    done
done


for REPO in "ray" "ray-ml" "autoscaler" "ray-deps" "base-deps"
do
    docker pull rayproject/"$REPO":"$IMAGE"
    docker tag rayproject/"$REPO":"$IMAGE" rayproject/"$REPO":"$DEST"
    docker tag rayproject/"$REPO":"$IMAGE" rayproject/"$REPO":"$DEST"-cpu

    docker pull rayproject/"$REPO":"$IMAGE"-gpu
    docker tag rayproject/"$REPO":"$IMAGE"-gpu rayproject/"$REPO":"$DEST"-gpu
    
    docker push rayproject/"$REPO":"$DEST"
    docker push rayproject/"$REPO":"$DEST"-cpu
    docker push rayproject/"$REPO":"$DEST"-gpu
done

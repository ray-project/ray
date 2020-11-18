#!/bin/bash
# This script is not for normal use and is used in the event that CI (or a user) overwrites the latest tag.
set -x

IMAGE="1.0.0"
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

echo "You must be logged into a user with push priviledges to do this."

docker pull rayproject/ray:"$IMAGE"
docker tag rayproject/ray:"$IMAGE" rayproject/ray:"$DEST"
docker tag rayproject/ray:"$IMAGE" rayproject/ray:"$DEST"-cpu
docker pull rayproject/ray:"$IMAGE"-gpu
docker tag rayproject/ray:"$IMAGE"-gpu rayproject/ray:"$DEST"-gpu
docker push rayproject/ray:"$DEST"
docker push rayproject/ray:"$DEST"-cpu
docker push rayproject/ray:"$DEST"-gpu

docker pull rayproject/ray-deps:"$IMAGE"
docker tag rayproject/ray-deps:"$IMAGE" rayproject/ray-deps:"$DEST"
docker tag rayproject/ray-deps:"$IMAGE" rayproject/ray-deps:"$DEST"-cpu
docker pull rayproject/ray-deps:"$IMAGE"-gpu
docker tag rayproject/ray-deps:"$IMAGE"-gpu rayproject/ray-deps:"$DEST"-gpu
docker push rayproject/ray-deps:"$DEST"
docker push rayproject/ray-deps:"$DEST"-cpu
docker push rayproject/ray-deps:"$DEST"-gpu

docker pull rayproject/base-deps:"$IMAGE"
docker tag rayproject/base-deps:"$IMAGE" rayproject/base-deps:"$DEST"
docker tag rayproject/base-deps:"$IMAGE" rayproject/base-deps:"$DEST"-cpu
docker pull rayproject/base-deps:"$IMAGE"-gpu
docker tag rayproject/base-deps:"$IMAGE"-gpu rayproject/base-deps:"$DEST"-gpu
docker push rayproject/base-deps:"$DEST"
docker push rayproject/base-deps:"$DEST"-cpu
docker push rayproject/base-deps:"$DEST"-gpu

docker pull rayproject/ray-ml:"$IMAGE"
docker tag rayproject/ray-ml:"$IMAGE" rayproject/ray-ml:"$DEST"
docker tag rayproject/ray-ml:"$IMAGE" rayproject/ray-ml:"$DEST"-cpu
docker pull rayproject/ray-ml:"$IMAGE"-gpu
docker tag rayproject/ray-ml:"$IMAGE"-gpu rayproject/ray-ml:"$DEST"-gpu
docker push rayproject/ray-ml:"$DEST"
docker push rayproject/ray-ml:"$DEST"-cpu
docker push rayproject/ray-ml:"$DEST"-gpu

docker pull rayproject/autoscaler:"$IMAGE"
docker tag rayproject/autoscaler:"$IMAGE" rayproject/autoscaler:"$DEST"
docker tag rayproject/autoscaler:"$IMAGE" rayproject/autoscaler:"$DEST"-cpu
docker pull rayproject/autoscaler:"$IMAGE"-gpu
docker tag rayproject/autoscaler:"$IMAGE"-gpu rayproject/autoscaler:"$DEST"-gpu
docker push rayproject/autoscaler:"$DEST"
docker push rayproject/autoscaler:"$DEST"-cpu
docker push rayproject/autoscaler:"$DEST"-gpu

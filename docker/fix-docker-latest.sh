#!/bin/bash
# This script is not for normal use and is used in the event that CI (or a user) overwrites the latest tag.

IMAGE="1.0.0"

while [[ $# -gt 0 ]]
do
key="$1"
case $key in
    --latest-tag)
    shift
    IMAGE=$1
    ;;
    *)
    echo "Usage: fix-docker-latest.sh --latest-tag <TAG>"
    exit 1
esac
shift
done

echo "You must be logged into a user with push priviledges to do this."

docker pull rayproject/ray:"$IMAGE"
docker tag rayproject/ray:"$IMAGE" rayproject/ray:latest
docker tag rayproject/ray:"$IMAGE" rayproject/ray:latest-cpu
docker pull rayproject/ray:"$IMAGE"-gpu
docker tag rayproject/ray:"$IMAGE"-gpu rayproject/ray:latest-gpu
docker push rayproject/ray:latest
docker push rayproject/ray:latest-cpu
docker push rayproject/ray:latest-gpu

docker pull rayproject/ray-deps:"$IMAGE"
docker tag rayproject/ray-deps:"$IMAGE" rayproject/ray-deps:latest
docker tag rayproject/ray-deps:"$IMAGE" rayproject/ray-deps:latest-cpu
docker pull rayproject/ray-deps:"$IMAGE"-gpu
docker tag rayproject/ray-deps:"$IMAGE"-gpu rayproject/ray-deps:latest-gpu
docker push rayproject/ray-deps:latest
docker push rayproject/ray-deps:latest-cpu
docker push rayproject/ray-deps:latest-gpu

docker pull rayproject/base-deps:"$IMAGE"
docker tag rayproject/base-deps:"$IMAGE" rayproject/base-deps:latest
docker tag rayproject/base-deps:"$IMAGE" rayproject/base-deps:latest-cpu
docker pull rayproject/base-deps:"$IMAGE"-gpu
docker tag rayproject/base-deps:"$IMAGE"-gpu rayproject/base-deps:latest-gpu
docker push rayproject/base-deps:latest
docker push rayproject/base-deps:latest-cpu
docker push rayproject/base-deps:latest-gpu

docker pull rayproject/ray-ml:"$IMAGE"
docker tag rayproject/ray-ml:"$IMAGE" rayproject/ray-ml:latest
docker tag rayproject/ray-ml:"$IMAGE" rayproject/ray-ml:latest-cpu
docker pull rayproject/ray-ml:"$IMAGE"-gpu
docker tag rayproject/ray-ml:"$IMAGE"-gpu rayproject/ray-ml:latest-gpu
docker push rayproject/ray-ml:latest
docker push rayproject/ray-ml:latest-cpu
docker push rayproject/ray-ml:latest-gpu

docker pull rayproject/autoscaler:"$IMAGE"
docker tag rayproject/autoscaler:"$IMAGE" rayproject/autoscaler:latest
docker tag rayproject/autoscaler:"$IMAGE" rayproject/autoscaler:latest-cpu
docker pull rayproject/autoscaler:"$IMAGE"-gpu
docker tag rayproject/autoscaler:"$IMAGE"-gpu rayproject/autoscaler:latest-gpu
docker push rayproject/autoscaler:latest
docker push rayproject/autoscaler:latest-cpu
docker push rayproject/autoscaler:latest-gpu

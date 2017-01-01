#!/bin/bash

docker build -t ray-project/base-deps docker/base-deps

tar --exclude './docker' -c . > ./docker/deploy/ray.tar
docker build --no-cache -t ray-project/deploy docker/deploy
rm ./docker/deploy/ray.tar
docker build -t ray-project/examples docker/examples

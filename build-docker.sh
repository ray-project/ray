#!/bin/bash

docker build -t ray-project/base-deps docker/base-deps

git rev-parse HEAD > ./docker/deploy/git-rev
git archive -o ./docker/deploy/ray.tar $(git rev-parse HEAD)
docker build --no-cache -t ray-project/deploy docker/deploy
rm ./docker/deploy/ray.tar ./docker/deploy/git-rev
docker build -t ray-project/examples docker/examples

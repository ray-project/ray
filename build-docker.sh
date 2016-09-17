#!/bin/bash

docker build -t ray-project/ray:devel docker/devel
docker build -t ray-project/ray:deploy docker/deploy
docker build -t ray-project/ray:examples docker/examples

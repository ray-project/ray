#!/bin/bash

DOCKER_SHA=$(./build-docker.sh --output-sha --no-cache --skip-examples)

echo "user Docker image" $DOCKER_SHA
python test/docker_test.py --docker-image=$DOCKER_SHA test/test_0.py
python test/docker_test.py --docker-image=$DOCKER_SHA test/test_1.py

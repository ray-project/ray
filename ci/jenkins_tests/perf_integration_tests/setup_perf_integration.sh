#!/usr/bin/env bash

set -ex

sha_tag=`git rev-parse --verify --short HEAD`
docker build -t ray-project/perf-test:$sha_tag -f docker/performance_test/Dockerfile .
docker run --rm --shm-size=5g ray-project/perf-test:$sha_tag

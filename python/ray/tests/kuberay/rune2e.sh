#!/bin/bash

set -x
RAY_IMAGE=rayproject/autoscaling_e2e_test_image
kind delete cluster
docker image rm $RAY_IMAGE

pushd ../../../..
docker build --progress=plain --build-arg BUILD_DATE="$(date +%Y-%m-%d:%H:%M:%S)" -t $RAY_IMAGE -f ./python/ray/tests/kuberay/Dockerfile . || exit
popd || exit

kind create cluster || exit
kind load docker-image $RAY_IMAGE || exit
python setup/setup_kuberay.py
RAY_IMAGE=$RAY_IMAGE python test_autoscaling_e2e.py

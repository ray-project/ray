#!/bin/bash

set -x
RAY_IMAGE_BUILT=rayproject/autoscaling_e2e_test_image_built
kind delete cluster

pushd ../../../..
docker build -t $RAY_IMAGE_BUILT -f ./python/ray/tests/kuberay/Dockerfile.built . || exit
popd || exit

kind create cluster || exit
kind load docker-image $RAY_IMAGE_BUILT || exit
python setup/setup_kuberay.py
RAY_IMAGE=$RAY_IMAGE_BUILT pytest -vvs test_autoscaling_e2e.py

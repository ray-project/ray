#!/bin/bash

set -euo pipefail

# TODO(aslonnie): refactor this test to be hermetic.

PYTHON_VERSION=3.10

echo "--- Build image"
bazel run //ci/ray_ci:build_in_docker -- docker \
    --python-version "$PYTHON_VERSION" \
    --platform cpu --canonical-tag kuberay-test
docker tag rayproject/ray:kuberay-test ray-ci:kuberay-test

# Joblib is an optional Ray integration and is not installed in the regular Ray
# image. Add only Ray's pinned Joblib test dependency to the image used by this
# suite so the Joblib autoscaling workload can run inside the head pod.
docker build \
    --build-arg RAY_BASE_IMAGE=ray-ci:kuberay-test \
    --file python/ray/tests/kuberay/joblib_autoscaling.Dockerfile \
    --tag ray-ci:kuberay-test-with-joblib \
    .

echo "--- Setup k8s environment"
bash ci/k8s/prep-k8s-environment.sh
kind load docker-image ray-ci:kuberay-test-with-joblib

# The following is essentially running
# python python/ray/tests/kuberay/setup/setup_kuberay.py

bash python/ray/autoscaler/kuberay/init-config.sh
kubectl create namespace kuberay-system
kubectl create -k python/ray/autoscaler/kuberay/config/default

echo "--- Test ray cluster creation"
kubectl apply -f python/ray/tests/kuberay/setup/raycluster_test.yaml
kubectl get rayclusters.ray.io
kubectl delete -f python/ray/tests/kuberay/setup/raycluster_test.yaml

echo "--- Wait until all pods of test cluster are deleted"
kubectl get pods -o custom-columns=POD:metadata.name --no-headers

for i in {1..120}; do
    if [[ "$(kubectl get pods -o custom-columns=POD:metadata.name --no-headers | wc -l)" == "0" ]]; then
        break
    fi
    if [[ $i == 120 ]]; then
        echo "Timed out waiting for pods to be deleted"
        exit 1
    fi
    sleep 1
done

echo "--- Run bazel tests"

# Needs to send in the kubeconfig file in base64 encoding.

bazel run //ci/ray_ci:test_in_docker -- //python/ray/tests/... kuberay \
    --build-name k8sbuild \
    --network host \
    --test-env=RAY_IMAGE=docker.io/library/ray-ci:kuberay-test-with-joblib \
    --test-env=PULL_POLICY=Never \
    --test-env=KUBECONFIG=/tmp/rayci-kubeconfig \
    --python-version "$PYTHON_VERSION" \
    "--test-env=KUBECONFIG_BASE64=$(base64 -w0 "$HOME/.kube/config")"

# Test for autoscaler v2.
bazel run //ci/ray_ci:test_in_docker -- //python/ray/tests/... kuberay \
    --build-name k8sbuild \
    --network host \
    --test-env=RAY_IMAGE=docker.io/library/ray-ci:kuberay-test-with-joblib \
    --test-env=PULL_POLICY=Never \
    --test-env=AUTOSCALER_V2=True \
    --test-env=KUBECONFIG=/tmp/rayci-kubeconfig \
    --python-version "$PYTHON_VERSION" \
    "--test-env=KUBECONFIG_BASE64=$(base64 -w0 "$HOME/.kube/config")"

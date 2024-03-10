#!/bin/bash

set -euo pipefail

# TODO(aslonnie): refactor this test to be hermetic.

echo "--- Build image"
bazel run //ci/ray_ci:build_in_docker -- docker \
    --platform cpu --canonical-tag kuberay-test
docker tag rayproject/ray:kuberay-test ray-ci:kuberay-test

echo "--- Setup k8s environment"
bash ci/k8s/prep-k8s-environment.sh
kind load docker-image ray-ci:kuberay-test

# The following is essentially running
# python python/ray/tests/kuberay/setup/setup_kuberay.py

bash python/ray/autoscaler/kuberay/init-config.sh
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
    --test-env=RAY_IMAGE=docker.io/library/ray-ci:kuberay-test \
    --test-env=PULL_POLICY=Never \
    --test-env=KUBECONFIG=/tmp/rayci-kubeconfig \
    "--test-env=KUBECONFIG_BASE64=$(base64 -w0 "$HOME/.kube/config")"

# Test for autoscaler v2.
bazel run //ci/ray_ci:test_in_docker -- //python/ray/tests/... kuberay \
    --build-name k8sbuild \
    --network host \
    --test-env=RAY_IMAGE=docker.io/library/ray-ci:kuberay-test \
    --test-env=PULL_POLICY=Never \
    --test-env=AUTOSCALER_V2=True \
    --test-env=KUBECONFIG=/tmp/rayci-kubeconfig \
    "--test-env=KUBECONFIG_BASE64=$(base64 -w0 "$HOME/.kube/config")"

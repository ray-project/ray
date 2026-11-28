#!/bin/bash

set -euo pipefail

# TODO(aslonnie): refactor this test to be hermetic.

PYTHON_VERSION=3.10

bash ci/k8s/setup-kuberay.sh "$PYTHON_VERSION"

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
    --python-version "$PYTHON_VERSION" \
    "--test-env=KUBECONFIG_BASE64=$(base64 -w0 "$HOME/.kube/config")"

# Test for autoscaler v2.
bazel run //ci/ray_ci:test_in_docker -- //python/ray/tests/... kuberay \
    --build-name k8sbuild \
    --network host \
    --test-env=RAY_IMAGE=docker.io/library/ray-ci:kuberay-test \
    --test-env=PULL_POLICY=Never \
    --test-env=AUTOSCALER_V2=True \
    --test-env=KUBECONFIG=/tmp/rayci-kubeconfig \
    --python-version "$PYTHON_VERSION" \
    "--test-env=KUBECONFIG_BASE64=$(base64 -w0 "$HOME/.kube/config")"

#!/bin/bash
#
# Deletes Ray head and worker Pods while a RayService serves traffic, and checks
# that it keeps serving. See python/ray/serve/tests/kuberay/serve_ha.py.
set -euo pipefail
trap 'kind delete clusters --all' EXIT
bash ci/k8s/setup-kuberay.sh 3.10
echo "--- Wait for the KubeRay operator"
kubectl wait -n kuberay-system --for=condition=Available deployment/kuberay-operator \
    --timeout=5m
# After the operator, so the CRD has conditions: `kubectl wait` fails on an object without any.
kubectl wait --for=condition=Established crd/rayservices.ray.io --timeout=2m
echo "--- Run the test"
python python/ray/serve/tests/kuberay/serve_ha.py

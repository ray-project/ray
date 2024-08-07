#!/usr/bin/env bash
#
# Sets up environment for the Kubernetes chaos testing.
# The environment consists of:
# - a Redis service with password.
# - a KubeRay cluster with Fault Tolerance enabled on the Redis service, port-forwarded
#     to localhost:8265.

set -euo pipefail

bash python/ray/tests/chaos/install_deps.sh

echo "--- Installing KubeRay FT cluster and port forward."

kubectl apply -f python/ray/tests/chaos/kuberay_cluster_ft.yaml

kubectl wait pod -l ray.io/cluster=raycluster-kuberay \
    --for=condition=Ready=True --timeout=5m
kubectl port-forward service/raycluster-kuberay-head-svc 8265:8265 &

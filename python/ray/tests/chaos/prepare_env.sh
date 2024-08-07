#!/usr/bin/env bash
#
# Sets up environment for the Kubernetes chaos testing.
# The environment consists of:
# - a KubeRay cluster, port-forwarded to localhost:8265.
# - a chaos-mesh operator ready to inject faults.

set -euo pipefail

bash python/ray/tests/chaos/install_deps.sh

echo "--- Installing KubeRay cluster and port forward."

helm install raycluster kuberay/ray-cluster \
    --set image.repository=ray-ci \
    --set image.tag=kuberay-test \
    --set worker.replicas=2 \
    --set worker.resources.limits.cpu=500m \
    --set worker.resources.requests.cpu=500m \
    --set head.resources.limits.cpu=500m \
    --set head.resources.requests.cpu=500m

kubectl wait pod -l ray.io/cluster=raycluster-kuberay \
    --for=condition=Ready=True --timeout=5m
kubectl port-forward service/raycluster-kuberay-head-svc 8265:8265 &

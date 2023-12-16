#!/usr/bin/env bash
#
# Sets up environment for the Kubernetes chaos testing.
# The environment consists of:
# - a KubeRay cluster, port-forwarded to localhost:8265.
# - a chaos-mesh operator ready to inject faults.

set -euo pipefail

echo "--- Preparing k8s environment."
bash ci/k8s/prep-k8s-environment.sh

kind load docker-image ray-ci:kuberay-test

# Helm install KubeRay
echo "--- Installing KubeRay operator from official Helm repo."
helm repo add kuberay https://ray-project.github.io/kuberay-helm/
helm install kuberay-operator kuberay/kuberay-operator
kubectl wait pod  -l app.kubernetes.io/name=kuberay-operator \
    --for=condition=Ready=True  --timeout=5m

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

# Helm install chaos-mesh
echo "--- Installing chaos-mesh operator and CR."
helm repo add chaos-mesh https://charts.chaos-mesh.org
kubectl create ns chaos-mesh
helm install chaos-mesh chaos-mesh/chaos-mesh -n=chaos-mesh \
    --set chaosDaemon.runtime=containerd \
    --set chaosDaemon.socketPath=/run/containerd/containerd.sock \
    --version 2.6.1
kubectl wait pod --namespace chaos-mesh \
    -l app.kubernetes.io/instance=chaos-mesh --for=condition=Ready=True

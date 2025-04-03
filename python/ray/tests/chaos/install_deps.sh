#!/usr/bin/env bash
#
# Sets up environment for the Kubernetes chaos testing.
# The environment consists of:
# - a KubeRay operator
# - a chaos-mesh operator ready to inject faults.
# Note there's no RayCluster.

set -euo pipefail

echo "--- Preparing k8s environment."
bash ci/k8s/prep-k8s-environment.sh

kind load docker-image ray-ci:kuberay-test

# Helm install KubeRay
echo "--- Installing KubeRay operator from official Helm repo."
helm repo add kuberay https://ray-project.github.io/kuberay-helm/
helm install kuberay-operator kuberay/kuberay-operator
kubectl wait pod  -l app.kubernetes.io/name=kuberay-operator \
    --for=condition=Ready=True  --timeout=2m

# Helm install chaos-mesh
echo "--- Installing chaos-mesh operator and CR."
helm repo add chaos-mesh https://charts.chaos-mesh.org
kubectl create ns chaos-mesh
helm install chaos-mesh chaos-mesh/chaos-mesh -n=chaos-mesh \
    --set chaosDaemon.runtime=containerd \
    --set chaosDaemon.socketPath=/run/containerd/containerd.sock \
    --version 2.6.1

echo "--- Waiting for chaos-mesh to be ready."
kubectl wait pod --namespace chaos-mesh --timeout=300s \
    -l app.kubernetes.io/instance=chaos-mesh --for=condition=Ready=True


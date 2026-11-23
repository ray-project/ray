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
    --for=condition=Ready=True  --timeout=2m

echo "--- Installing KubeRay cluster and port forward."

# Ray defaults to token auth, so `ray start --head` exits when the pod has no
# token. Turn on KubeRay-managed auth, which generates the token
# and injects it into every Ray container.
# KubeRay accepts `authOptions` only with rayVersion >= 2.52.0.
helm template raycluster kuberay/ray-cluster \
    --set image.repository=ray-ci \
    --set image.tag=kuberay-test \
    --set head.rayVersion=2.52.0 \
    --set worker.replicas=2 \
    --set worker.resources.limits.cpu=500m \
    --set worker.resources.requests.cpu=500m \
    --set head.resources.limits.cpu=500m \
    --set head.resources.requests.cpu=500m \
    --set worker.resources.limits.memory=4Gi \
    --set worker.resources.requests.memory=4Gi \
    --set head.resources.limits.memory=4Gi \
    --set head.resources.requests.memory=4Gi \
    | kubectl patch --local -f - --type merge -o yaml \
        -p '{"spec":{"authOptions":{"mode":"token"}}}' \
    | kubectl apply -f -

# Wait for the operator to create the pods; `kubectl wait` fails at once when
# no pod matches the selector yet.
for _ in {1..60}; do
    if [[ -n "$(kubectl get pod -l ray.io/cluster=raycluster-kuberay -o name)" ]]; then
        break
    fi
    sleep 2
done
if ! kubectl wait pod -l ray.io/cluster=raycluster-kuberay \
    --for=condition=Ready=True --timeout=5m; then
    kubectl get raycluster,pods -o wide || true
    kubectl describe raycluster raycluster-kuberay || true
    kubectl logs -l ray.io/cluster=raycluster-kuberay --all-containers \
        --prefix --tail=50 || true
    kubectl logs -l ray.io/cluster=raycluster-kuberay --all-containers \
        --prefix --tail=50 --previous || true
    exit 1
fi
kubectl port-forward service/raycluster-kuberay-head-svc 8265:8265 &

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

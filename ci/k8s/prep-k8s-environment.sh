#!/usr/bin/env bash

# This scripts creates a kind cluster and verify it works

set -euo pipefail

# Install kind

if [[ ! -f /usr/local/bin/kind ]]; then
    echo "--- Installing kind"
    curl -sfL "https://github.com/kubernetes-sigs/kind/releases/download/v0.11.1/kind-linux-amd64" -o /usr/local/bin/kind
    chmod +x /usr/local/bin/kind
    kind --help
fi 

if [[ ! -f /usr/local/bin/kubectl ]]; then
    echo "--- Installing kubectl"
    curl -sfL "https://dl.k8s.io/release/v1.28.4/bin/linux/amd64/kubectl" -o /usr/local/bin/kubectl
    chmod +x /usr/local/bin/kubectl
    kubectl version --client
fi

if [[ ! -f /usr/local/bin/kustomize ]]; then
    echo "--- Installing kustomize"
    curl -sfL "https://github.com/kubernetes-sigs/kustomize/releases/download/kustomize%2Fv5.2.1/kustomize_v5.2.1_linux_amd64.tar.gz" \
        | tar -xzf - -C /usr/local/bin kustomize
fi

set -x # Be more verbose now.

# Delete dangling clusters
kind delete clusters --all

# Create the cluster
time kind create cluster --wait 120s --config ./ci/k8s/kind.config.yaml
docker ps

# Now the kind node is running, it exposes port 6443 in the dind-daemon network.
kubectl config set clusters.kind-kind.server https://docker:6443

# Verify the kubectl works
kubectl version
kubectl cluster-info
kubectl get nodes
kubectl get pods --all-namespaces


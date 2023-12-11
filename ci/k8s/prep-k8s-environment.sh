#!/usr/bin/env bash

# This scripts creates a kind cluster and verify it works

set -exo pipefail

# Install kind
curl -sfL "https://github.com/kubernetes-sigs/kind/releases/download/v0.11.1/kind-linux-amd64" -o kind-linux-amd64
chmod +x kind-linux-amd64
mv ./kind-linux-amd64 /usr/bin/kind
kind --help

# Install kubectl
curl -sfL "https://dl.k8s.io/release/v1.28.4/bin/linux/amd64/kubectl" -o kubectl
chmod +x kubectl
mv ./kubectl /usr/bin/kubectl
kubectl version --client

curl -sfL "https://github.com/kubernetes-sigs/kustomize/releases/download/kustomize%2Fv5.2.1/kustomize_v5.2.1_linux_amd64.tar.gz" | tar -xzf - kustomize
mv ./kustomize /usr/bin/kustomize

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


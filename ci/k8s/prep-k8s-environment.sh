#!/usr/bin/env bash

# This scripts creates a kind cluster and verify it works

set -euo pipefail

# Install k8s tools
bash ci/k8s/install-k8s-tools.sh

set -x # Be more verbose now.

# Delete dangling clusters
kind delete clusters --all

kind create cluster --wait 120s --config ci/k8s/kind.config.yaml

# Verify the kubectl works
docker ps
kubectl version
kubectl cluster-info
kubectl get nodes
kubectl get pods --all-namespaces

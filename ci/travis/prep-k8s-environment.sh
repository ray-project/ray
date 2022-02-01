#!/usr/bin/env bash

set -ex

GO111MODULE="on" go get sigs.k8s.io/kind@v0.11.1

curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
chmod +x kubectl
mkdir -p ~/.local/bin/kubectl
mv ./kubectl ~/.local/bin/kubectl
kubectl version --client

time kind create cluster
kubectl cluster-info --context kind-kind
kubectl get nodes
kubectl get pods

#!/usr/bin/env bash

set -ex

# Install kind
wget https://github.com/kubernetes-sigs/kind/releases/download/v0.11.1/kind-linux-amd64
chmod +x kind-linux-amd64
mv ./kind-linux-amd64 ~/.local/bin
kind --help

# Install kubectl
curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
chmod +x kubectl
mkdir -p ~/.local/bin/kubectl
mv ./kubectl ~/.local/bin/kubectl
kubectl version --client

time kind create cluster
kubectl cluster-info --context kind-kind
kubectl get nodes
kubectl get pods

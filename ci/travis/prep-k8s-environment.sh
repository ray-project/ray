#!/usr/bin/env bash

set -ex

# Install kind
wget https://github.com/kubernetes-sigs/kind/releases/download/v0.11.1/kind-linux-amd64
chmod +x kind-linux-amd64
mv ./kind-linux-amd64 /usr/bin/kind
kind --help

#Install kubectl
#curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
#chmod +x kubectl
#mv ./kubectl /usr/bin/kubectl
#kubectl version --client

# https://github.com/kubernetes-sigs/kind/issues/273#issuecomment-622180144
#export KIND_EXPERIMENTAL_DOCKER_NETWORK=dind-network
time kind create cluster --wait 120s --config ./ci/travis/kind.config.yaml
shopt -s expand_aliases
alias kubectl="docker run --network kind bitnami/kubectl:latest"
kubectl cluster-info --context kind-kind
kubectl get nodes
kubectl get pods

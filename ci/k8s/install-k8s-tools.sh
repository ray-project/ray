#!/usr/bin/env bash

set -euo pipefail

if [[ ! -f /usr/local/bin/kind ]]; then
    echo "--- Installing kind"
    curl -sfL "https://github.com/kubernetes-sigs/kind/releases/download/v0.22.0/kind-linux-amd64" -o /usr/local/bin/kind
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

if [[ ! -d /usr/local/helm ]]; then
    echo "--- Installing helm"
    mkdir -p /usr/local/helm
    curl -sfL "https://get.helm.sh/helm-v3.12.2-linux-amd64.tar.gz" | tar -xzf - -C /usr/local/helm linux-amd64/helm
    ln -s /usr/local/helm/linux-amd64/helm /usr/local/bin/helm
fi

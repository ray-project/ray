#!/usr/bin/env bash

KUBERAY_VERSION="1.4.0"

alias jq='jq --monochrome-output'

install_kuberay_operator() {
  if ! helm repo list | grep -q '^kuberay\s'; then
    helm repo add kuberay https://ray-project.github.io/kuberay-helm/
    helm repo update
  fi
  helm install kuberay-operator kuberay/kuberay-operator --version "${KUBERAY_VERSION}"
  kubectl wait --for=condition=available deployment/kuberay-operator --timeout=120s
}

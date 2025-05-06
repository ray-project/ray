#!/usr/bin/env bash

KUBERAY_VERSION="1.3.0"

script_name=$(basename "$0")

if [ "$#" -eq 0 ]; then
  echo "Usage: ${script_name} <command>" >&2
  exit 1
fi

kuberay_version() {
  echo "${KUBERAY_VERSION}"
}

install_kuberay_operator() {
  if ! helm repo list | grep -q '^kuberay\s'; then
    helm repo add kuberay https://ray-project.github.io/kuberay-helm/
    helm repo update
  fi
  helm install kuberay-operator kuberay/kuberay-operator --version "${KUBERAY_VERSION}"
  kubectl wait --for=condition=available deployment/kuberay-operator --timeout=120s
}

case "$1" in
  kuberay_version)
    kuberay_version
    ;;
  install_kuberay_operator)
    install_kuberay_operator
    ;;
  *)
    echo "Error: No such command: '$1'" >&2
    exit 1
    ;;
esac

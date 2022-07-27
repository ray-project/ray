#!/bin/bash

# Clone pinned Kuberay commit to temporary directory, copy the CRD definitions
# into the autoscaler folder.

KUBERAY_SHA="ce84f0441c991eb4b0f52ee2cd85c0a5ac048d11"
OPERATOR_TAG=${KUBERAY_SHA:0:7}

# Requires Kustomize (dependency to be removed after KubeRay 0.3.0 cut)
if ! command -v kustomize &> /dev/null
then
    echo "Please install kustomize. Then re-run this script."
    exit
fi

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

DIR=$(mktemp -d -t "kuberay-XXXXXX")

pushd "$DIR" || exit
    git clone https://github.com/ray-project/kuberay/
    pushd "kuberay" || exit
        git checkout "$KUBERAY_SHA$"
        pushd ray-operator/config/default || exit
            kustomize edit set image kuberay/operator=kuberay/operator:"$OPERATOR_TAG"
        popd || exit
        cp -r ray-operator/config "$SCRIPT_DIR/"
    popd || exit
popd || exit

#!/bin/bash

# Clone pinned Kuberay commit to temporary directory, copy the CRD definitions
# into the autoscaler folder.
KUBERAY_COMMIT="v0.3.0-rc.2"
OPERATOR_TAG="v0.3.0-rc.2"

# Requires Kustomize (dependency to be removed after KubeRay 1.3.0 cut)
if ! command -v kustomize &> /dev/null
then
    echo "Please install kustomize. Then re-run this script."
    exit
fi

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

DIR=$(mktemp -d -t "kuberay-XXXXXX")

pushd "$DIR" || exit
    git clone https://github.com/ray-project/kuberay/ --branch "$KUBERAY_COMMIT" --depth 1
    pushd kuberay/ray-operator/config/default || exit
        kustomize edit set image kuberay/operator=kuberay/operator:"$OPERATOR_TAG"
    popd || exit
    cp -r kuberay/ray-operator/config "$SCRIPT_DIR/"
popd || exit

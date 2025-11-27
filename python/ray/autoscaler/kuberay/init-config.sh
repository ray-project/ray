#!/bin/bash

set -euo pipefail

# Clone pinned KubeRay commit to temporary directory, copy the CRD definitions
# into the autoscaler folder.
KUBERAY_BRANCH="v1.5.0"
OPERATOR_TAG="v1.5.0"

# Requires Kustomize
if ! command -v kustomize &> /dev/null
then
    echo "Please install kustomize. Then re-run this script."
    exit
fi

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

DIR=$(mktemp -d -t "kuberay-XXXXXX")

(
    cd "$DIR"
    git clone https://github.com/ray-project/kuberay/ --branch "$KUBERAY_BRANCH" --depth 1
    (
        cd kuberay/ray-operator/config/default
        kustomize edit set image kuberay/operator=quay.io/kuberay/operator:"$OPERATOR_TAG"
        kustomize edit set namespace kuberay-system
    )
    cp -r kuberay/ray-operator/config "$SCRIPT_DIR/"
)

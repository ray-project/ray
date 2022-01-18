#!/bin/bash

# Clone pinned Kuberay commit to temporary directory, copy the CRD definitions
# into the autoscaler folder.

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

DIR=$(mktemp -d -t "kuberay-XXXXXX")

pushd "$DIR" || exit
    git clone https://github.com/ray-project/kuberay/
    pushd "kuberay" || exit
        git checkout 6f87ca64c107cd51d3ab955faf4be198e0094536
        # This would normally better be done with kustomization, but we don't want to make
        # kustomization a dependency for running this.
        git apply "$SCRIPT_DIR/kuberay-autoscaler.patch"
        cp -r ray-operator/config "$SCRIPT_DIR/"
    popd || exit
popd || exit

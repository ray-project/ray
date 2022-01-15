#!/bin/bash

# Clone pinned Kuberay commit to temporary directory, copy the CRD definitions
# into the autoscaler folder.

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

DIR=$(mktemp -d -t "kuberay-XXXXXX")

pushd "$DIR"
    git clone https://github.com/ray-project/kuberay/
    pushd "kuberay"
        git checkout 6f87ca64c107cd51d3ab955faf4be198e0094536
        cp -r ray-operator/config $SCRIPT_DIR/
    popd
popd
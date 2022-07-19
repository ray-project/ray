#!/bin/bash

# Clone pinned Kuberay commit to temporary directory, copy the CRD definitions
# into the autoscaler folder.

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

DIR=$(mktemp -d -t "kuberay-XXXXXX")

pushd "$DIR" || exit
    git clone https://github.com/ray-project/kuberay/
    pushd "kuberay" || exit
        # If you changed the Kuberay CRD, you need to update this commit to point
        # to the new CRD. The following always need to be compatible: The used CRDs,
        # the docker image of the Kuberay operator and the KuberayNodeProvider.
        # This is normally not a problem since the KuberayNodeProvider uses a
        # stable part of the CRD definition and the Kuberay operator and the
        # CRDs are in the https://github.com/ray-project/kuberay/ so they
        # get updated together. It is important to keep this in mind when making
        # changes. The CRD is designed to be stable so one operator can run many
        # different versions of Ray.
        git checkout 69ecfceef5c966193ab87f22a9f49250b17e35fb
        # Here is where we specify the docker image that is used for the operator.
        # If you want to use your own version of Kuberay, you should change the content
        # of kuberay-autoscaler.patch to point to your operator.
        # This would normally better be done with kustomization, but we don't want to make
        # kustomization a dependency for running this.
        git apply "$SCRIPT_DIR/kuberay-autoscaler.patch"
        cp -r ray-operator/config "$SCRIPT_DIR/"
    popd || exit
popd || exit

#!/bin/bash
set -x
IMAGE="$IMAGE" bash k8s-test.sh
BASIC_SUCCEEDED=$?
IMAGE="$IMAGE" bash helm-test.sh
HELM_SUCCEEDED=$?
IMAGE="$IMAGE" bash k8s-test-scale.sh
SCALE_SUCCEEDED=$?

if (( BASIC_SUCCEEDED == 0 ))
then
    echo "k8s-test.sh succeeded"
else
    echo "k8s-test.sh test failed"
fi

if (( HELM_SUCCEEDED == 0 ))
then
    echo "helm-test.sh test succeeded";
else
    echo "helm-test.sh test failed"
fi

if (( SCALE_SUCCEEDED == 0))
then
    echo "k8s-test-scale.sh test succeeded";
else
    echo "k8s-test-scale.sh failed. Try re-running just the k8s-test-scale.sh. It's expected to be flaky."
fi


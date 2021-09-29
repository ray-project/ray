#!/bin/bash
set -x
IMAGE="$IMAGE" bash k8s-test.sh
IMAGE="$IMAGE" bash helm-test.sh
IMAGE="$IMAGE" bash k8s-test-scale.sh

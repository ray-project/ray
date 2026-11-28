#!/bin/bash
#
# Builds the Ray image from this commit as ray-ci:kuberay-test, creates a kind
# cluster with the image loaded, and installs the KubeRay operator.
# Usage: setup-kuberay.sh <python version>
set -euo pipefail
PYTHON_VERSION="$1"
echo "--- Build image"
bazel run //ci/ray_ci:build_in_docker -- docker \
    --python-version "$PYTHON_VERSION" \
    --platform cpu --canonical-tag kuberay-test
docker tag rayproject/ray:kuberay-test ray-ci:kuberay-test
echo "--- Setup k8s environment"
bash ci/k8s/prep-k8s-environment.sh
kind load docker-image ray-ci:kuberay-test
# The following is essentially running
# python python/ray/tests/kuberay/setup/setup_kuberay.py
bash python/ray/autoscaler/kuberay/init-config.sh
kubectl create namespace kuberay-system
kubectl create -k python/ray/autoscaler/kuberay/config/default

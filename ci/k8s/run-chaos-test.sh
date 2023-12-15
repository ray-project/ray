#!/bin/bash

set -euo pipefail

CHAOS_FAULT="${1:-no_fault}"
CHAOS_WORKLOAD="${2:-test_potato_passer}"

bazel run //ci/ray_ci:build_in_docker -- docker --python-version 3.8 \
    --platform cpu --canonical-tag kuberay-test
docker tag rayproject/ray:kuberay-test ray-ci:kuberay-test

bash python/ray/tests/chaos/prepare_env.sh

if [[ "${CHAOS_FAULT}" != "no_fault" ]]; then
    kubectl apply -f "python/ray/tests/chaos/${CHAOS_FAULT}.yaml"
fi

echo "--- Prepare the test container."
docker run -d --network host --name chaos-test ray-ci:kuberay-test sleep infinity
docker exec -ti chaos-test mkdir -p /tmp/rayci/python/ray/tests/chaos
tar -cf - -C python/ray/tests/chaos . | docker cp - chaos-test:/tmp/rayci/python/ray/tests/chaos

echo "--- Run the test."
docker exec -w /tmp/rayci -ti chaos-test bash "python/ray/tests/chaos/${CHAOS_WORKLOAD}.sh"

docker rm -f chaos-test

kind delete clusters --all

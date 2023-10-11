#!/usr/bin/env bash
#
# Sets up environment for the Kubernetes chaos testing.
# The environment consists of:
# - a KubeRay cluster, port-forwarded to localhost:8265.
# - a chaos-mesh operator ready to inject faults.

set -xe

ray job submit --address http://localhost:8265 --runtime-env python/ray/tests/chaos/runtime_env.yaml --working-dir python/ray/tests/chaos -- python fine_tuning_like.py --num-actors=8 --iteration=5

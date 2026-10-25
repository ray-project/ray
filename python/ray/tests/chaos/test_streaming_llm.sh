#!/usr/bin/env bash
#
# Sets up environment for the Kubernetes chaos testing.
# The environment consists of:
# - a KubeRay cluster, port-forwarded to localhost:8265.
# - a chaos-mesh operator ready to inject faults.

set -xe

ray job submit --address http://localhost:8265 --runtime-env python/ray/tests/chaos/streaming_llm.yaml --working-dir python/ray/tests/chaos -- python streaming_llm.py --num_queries_per_task=100 --num_tasks=2 --num_words_per_query=100

"""Test entrypoint for symmetric_run multi-node integration test.

This script is executed by symmetric_run on the head node during
test_symmetric_run_three_node_cluster_simulated. It connects to the
Ray cluster and verifies that all expected nodes have joined.
"""
import sys
import time

import ray

EXPECTED_NODES = 3
TIMEOUT_SECONDS = 60

ray.init(address="auto", ignore_reinit_error=True, log_to_driver=False)

for _ in range(TIMEOUT_SECONDS):
    if len(ray.nodes()) >= EXPECTED_NODES:
        print("ENTRYPOINT_SUCCESS")
        sys.exit(0)
    time.sleep(1)

print(f"ENTRYPOINT_FAILED: cluster_size={len(ray.nodes())}")
sys.exit(1)

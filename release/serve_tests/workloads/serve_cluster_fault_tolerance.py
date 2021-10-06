"""
Test that a serve deployment can recover from cluster failures by resuming
from checkpoints of external source, such as s3.

For product testing, we skip the part of actually starting new cluster as
it's Job Manager's responsibility, and only re-deploy to the same cluster
with remote checkpoint.
"""

import click
import os
import time
import requests

from serve_test_cluster_utils import (setup_local_single_node_cluster,
                                      setup_anyscale_cluster)
from serve_test_utils import (
    save_test_results, )

import ray
from ray import serve
from ray.serve.utils import logger

# Deployment configs
DEFAULT_NUM_REPLICAS = 4
DEFAULT_MAX_BATCH_SIZE = 16

# Checkpoint configs
DEFAULT_CHECKPOINT_PATH = "s3://serve-nightly-tests/fault-tolerant-test-checkpoint"  # noqa: E501


def request_with_retries(endpoint, timeout=30):
    start = time.time()
    while True:
        try:
            return requests.get(
                "http://127.0.0.1:8000" + endpoint, timeout=timeout)
        except requests.RequestException:
            if time.time() - start > timeout:
                raise TimeoutError
            time.sleep(0.1)


@click.command()
def main():
    # (1) Setup cluster
    # IS_SMOKE_TEST is set by args of releaser's e2e.py
    smoke_test = os.environ.get("IS_SMOKE_TEST", "1")
    if smoke_test == "1":
        setup_local_single_node_cluster(
            1, checkpoint_path=DEFAULT_CHECKPOINT_PATH)
    else:
        setup_anyscale_cluster(checkpoint_path=DEFAULT_CHECKPOINT_PATH)

    # Deploy for the first time
    @serve.deployment(name="echo", num_replicas=DEFAULT_NUM_REPLICAS)
    class Echo:
        def __init__(self):
            return True

        def __call__(self, request):
            return "hii"

    Echo.deploy()

    # Ensure endpoint is working
    for _ in range(10):
        response = request_with_retries("/echo/", timeout=30)
        assert response.text == "hii"

    logger.info("Initial deployment successful with working endpoint.")

    # Kill controller and wait for endpoint to be available again
    # Recover from remote checkpoint
    # Ensure endpoint is still available with expected results
    ray.kill(serve.api._global_client._controller, no_restart=False)
    for _ in range(10):
        response = request_with_retries("/echo/", timeout=30)
        assert response.text == "hii"

    logger.info("Deployment recovery from s3 checkpoint is successful "
                "with working endpoint.")

    # Checkpoints in S3 bucket are moved after 7 days with explicit lifecycle
    # rules. Each checkpoint is ~260 Bytes in size from this test.

    # Save results
    save_test_results(
        {
            "result": "success"
        },
        default_output_file="/tmp/serve_cluster_fault_tolerance.json")


if __name__ == "__main__":
    main()
    import pytest
    import sys
    sys.exit(pytest.main(["-v", "-s", __file__]))

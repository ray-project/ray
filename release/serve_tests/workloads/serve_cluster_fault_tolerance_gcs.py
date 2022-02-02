"""
Test that a serve deployment can recover from cluster failures by resuming
from checkpoints of external source, such as Google Cloud Storage (GCS).

For product testing, we skip the part of actually starting new cluster as
it's Job Manager's responsibility, and only re-deploy to the same cluster
with remote checkpoint.
"""

import os
import time
import uuid

import click
import requests
from ray.serve.utils import logger
from serve_test_cluster_utils import setup_local_single_node_cluster
from serve_test_utils import save_test_results

import ray
from ray import serve

# Deployment configs
DEFAULT_NUM_REPLICAS = 4
DEFAULT_MAX_BATCH_SIZE = 16


def request_with_retries(endpoint, timeout=3):
    start = time.time()
    while True:
        try:
            return requests.get("http://127.0.0.1:8000" + endpoint, timeout=timeout)
        except requests.RequestException:
            if time.time() - start > timeout:
                raise TimeoutError
            time.sleep(0.1)


@click.command()
def main():
    # Setup local cluster, note this cluster setup is the same for both
    # local and product ray cluster env.
    # Each test uses different ray namespace, thus kv storage key for each
    # checkpoint is different to avoid collision.
    namespace = uuid.uuid4().hex

    # IS_SMOKE_TEST is set by args of releaser's e2e.py
    smoke_test = os.environ.get("IS_SMOKE_TEST", "1")
    if smoke_test == "1":
        checkpoint_path = "file://checkpoint.db"
    else:
        checkpoint_path = (
            "gs://kazi_test/test/fault-tolerant-test-checkpoint"  # noqa: E501
        )

    _, cluster = setup_local_single_node_cluster(
        1, checkpoint_path=checkpoint_path, namespace=namespace
    )

    # Deploy for the first time
    @serve.deployment(name="echo", num_replicas=DEFAULT_NUM_REPLICAS)
    class Echo:
        def __init__(self):
            return True

        def __call__(self, request):
            return "hii"

    Echo.deploy()

    # Ensure endpoint is working
    for _ in range(5):
        response = request_with_retries("/echo/", timeout=3)
        assert response.text == "hii"

    logger.info("Initial deployment successful with working endpoint.")

    # Kill current cluster, recover from remote checkpoint and ensure endpoint
    # is still available with expected results

    ray.kill(serve.api._global_client._controller, no_restart=True)
    ray.shutdown()
    cluster.shutdown()
    serve.api._set_global_client(None)

    # Start another ray cluster with same namespace to resume from previous
    # checkpoints with no new deploy() call.
    setup_local_single_node_cluster(
        1, checkpoint_path=checkpoint_path, namespace=namespace
    )

    for _ in range(5):
        response = request_with_retries("/echo/", timeout=3)
        assert response.text == "hii"

    logger.info(
        "Deployment recovery from Google Cloud Storage checkpoint "
        "is successful with working endpoint."
    )

    # Delete dangling checkpoints. If script failed before this step, it's up
    # to the TTL policy on GCS to clean up, but won't lead to collision with
    # subsequent tests since each test run in different uuid namespace.
    serve.shutdown()
    ray.shutdown()
    cluster.shutdown()

    # Checkpoints in GCS bucket are moved after 7 days with explicit lifecycle
    # rules. Each checkpoint is ~260 Bytes in size from this test.

    # Save results
    save_test_results(
        {"result": "success"},
        default_output_file="/tmp/serve_cluster_fault_tolerance.json",
    )


if __name__ == "__main__":
    main()
    import sys

    import pytest

    sys.exit(pytest.main(["-v", "-s", __file__]))

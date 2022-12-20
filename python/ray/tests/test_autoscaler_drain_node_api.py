import logging
import platform
import time

import pytest

import ray
import ray._private.ray_constants as ray_constants
from ray._private.test_utils import (
    get_error_message,
    init_error_pubsub,
    wait_for_condition,
)
from ray.autoscaler._private.fake_multi_node.node_provider import FakeMultiNodeProvider
from ray.cluster_utils import AutoscalingCluster

logger = logging.getLogger(__name__)


class MockFakeProvider(FakeMultiNodeProvider):
    """FakeMultiNodeProvider, with Ray node process termination mocked out.

    Used to check that a Ray node can be terminated by DrainNode API call
    from the autoscaler.
    """

    def _kill_ray_processes(self, node):
        logger.info("Leaving Raylet termination to autoscaler Drain API!")


class MockAutoscalingCluster(AutoscalingCluster):
    """AutoscalingCluster modified to used the above MockFakeProvider."""

    def _generate_config(self, head_resources, worker_node_types):
        config = super()._generate_config(head_resources, worker_node_types)
        config["provider"]["type"] = "external"
        config["provider"][
            "module"
        ] = "ray.tests.test_autoscaler_drain_node_api.MockFakeProvider"
        return config


@pytest.mark.skipif(platform.system() == "Windows", reason="Failing on Windows.")
def test_drain_api(shutdown_only):
    """E2E test of the autoscaler's use of the DrainNode API.

    Adapted from test_autoscaler_fake_multinode.py.

    The strategy is to mock out Ray node process termination in
    FakeMultiNodeProvider, leaving node termination to the DrainNode API.

    Scale-down is verified by `ray.cluster_resources`. It is verified that
    no removed_node errors are issued adter scale-down.

    Validity of this test depends on the current implementation of DrainNode.
    DrainNode currently works by asking the GCS to de-register and shut down
    Ray nodes.
    """
    # Autoscaling cluster with Ray process termination mocked out in the node
    # provider.
    cluster = MockAutoscalingCluster(
        head_resources={"CPU": 1},
        worker_node_types={
            "gpu_node": {
                "resources": {
                    "CPU": 1,
                    "GPU": 1,
                    "object_store_memory": 1024 * 1024 * 1024,
                },
                "node_config": {},
                "min_workers": 0,
                "max_workers": 2,
            },
        },
    )

    try:
        cluster.start()
        ray.init("auto")

        # Triggers the addition of a GPU node.
        @ray.remote(num_gpus=1)
        def f():
            print("gpu ok")

        ray.get(f.remote())

        # Verify scale-up
        wait_for_condition(lambda: ray.cluster_resources().get("GPU", 0) == 1)
        # Sleep for double the idle timeout of 6 seconds.
        time.sleep(12)

        # Verify scale-down
        wait_for_condition(lambda: ray.cluster_resources().get("GPU", 0) == 0)

        # Check that no errors were raised while draining nodes.
        # (Logic copied from test_failure4::test_gcs_drain.)
        try:
            p = init_error_pubsub()
            errors = get_error_message(
                p, 1, ray_constants.REMOVED_NODE_ERROR, timeout=5
            )
            assert len(errors) == 0
        finally:
            p.close()
    finally:
        cluster.shutdown()


if __name__ == "__main__":
    import os
    import sys

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))

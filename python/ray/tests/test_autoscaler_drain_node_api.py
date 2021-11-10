import platform
import time

import pytest

import ray
from ray.cluster_utils import AutoscalingCluster
import ray.ray_constants as ray_constants
from ray._private.test_utils import get_error_message, init_error_pubsub


@pytest.mark.skipif(
    platform.system() == "Windows", reason="Failing on Windows.")
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
    cluster = AutoscalingCluster(
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
        })

    try:
        cluster.start()
        ray.init("auto")

        # Triggers the addition of a GPU node.
        @ray.remote(num_gpus=1)
        def f():
            print("gpu ok")

        ray.get(f.remote())

        # Verify scale-up
        assert ray.cluster_resources().get("GPU", 0) == 1
        # Sleep for double the idle timeout of 6 seconds.
        time.sleep(12)

        # Verify scale-down
        assert ray.cluster_resources().get("GPU", 0) == 0

        # Check that no errors were raised while draining nodes.
        # (Logic copied from test_failure4::test_gcs_drain.)
        try:
            p = init_error_pubsub()
            errors = get_error_message(
                p, 1, ray_constants.REMOVED_NODE_ERROR, timeout=5)
            assert len(errors) == 0
        finally:
            p.close()
    finally:
        cluster.shutdown()


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))

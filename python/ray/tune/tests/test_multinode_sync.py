import sys
import time
import unittest

import ray
from ray.autoscaler._private.fake_multi_node.test_utils import DockerCluster


@ray.remote
def remote_task(val):
    return val


class MultiNodeSyncTest(unittest.TestCase):
    def setUp(self):
        self.cluster = DockerCluster()
        self.cluster.setup()

    def tearDown(self):
        self.cluster.stop()
        self.cluster.teardown()

    def testClusterAutoscaling(self):
        """Sanity check that multinode tests with autoscaling are working"""
        self.cluster.update_config({
            "provider": {
                "head_resources": {
                    "CPU": 4,
                    "GPU": 0
                }
            },
        })
        self.cluster.start()
        self.cluster.connect(client=True, timeout=120)

        self.assertGreater(ray.cluster_resources().get("CPU", 0), 0)

        # Trigger autoscaling
        pg = ray.util.placement_group([{"CPU": 1, "GPU": 1}] * 2)
        timeout = time.monotonic() + 120
        while ray.cluster_resources().get("GPU", 0) < 2:
            if time.monotonic() > timeout:
                raise RuntimeError("Autoscaling failed or too slow.")
            time.sleep(1)

        # Schedule task with resources
        self.assertEquals(
            5,
            ray.get(
                remote_task.options(
                    num_cpus=1, num_gpus=1, placement_group=pg).remote(5)))

        print("Autoscaling worked")


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main(["-v", __file__]))

import sys
import time
import unittest

import ray
from ray.autoscaler._private.fake_multi_node.test_utils import DockerCluster, \
    DockerMonitor


class MultiNodeSyncTest(unittest.TestCase):
    def setUp(self):

        self.cluster = DockerCluster()
        self.cluster.setup()

        self.monitor = DockerMonitor(self.cluster.config_file)
        self.monitor.start()

    def tearDown(self):
        self.cluster.stop()
        self.monitor.stop()
        self.cluster.teardown()

    def testClusterAutoscaling(self):
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

        pg = ray.util.placement_group([{"GPU": 1}] * 2)  # noqa: F841
        timeout = time.monotonic() + 60
        while ray.cluster_resources().get("GPU", 0) < 2:
            if time.monotonic() > timeout:
                raise RuntimeError("Autoscaling failed or too slow.")
            time.sleep(1)

        print("Autoscaling worked.")


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main(["-v", __file__]))

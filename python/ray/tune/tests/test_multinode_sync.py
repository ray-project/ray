import sys
import time
import unittest

import ray
from ray import tune
from ray.autoscaler._private.fake_multi_node.test_utils import DockerCluster
from ray.tune.callback import Callback
from ray.tune.trial import Trial


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
        ray.util.remove_placement_group(pg)

        time.sleep(2)  # Give some time so nodes.json is updated

        self.cluster.kill_node(num=2)
        print("Killed GPU node.")
        pg = ray.util.placement_group([{"CPU": 1, "GPU": 1}] * 2)

        table = ray.util.placement_group_table(pg)
        assert table["state"] == "PENDING"

        timeout = time.monotonic() + 180
        while table["state"] != "CREATED":
            if time.monotonic() > timeout:
                raise RuntimeError(
                    "Re-starting killed node failed or too slow.")
            time.sleep(1)
            table = ray.util.placement_group_table(pg)

        print("Node was restarted.")

    def testAutoscalingNewNode(self):
        """Test that newly added nodes from autoscaling are not stale."""
        self.cluster.update_config({
            "provider": {
                "head_resources": {
                    "CPU": 4,
                    "GPU": 0
                }
            },
            "available_node_types": {
                "ray.worker.cpu": {
                    "resources": {
                        "CPU": 4
                    },
                    "min_workers": 0,  # No minimum nodes
                    "max_workers": 2,
                },
                "ray.worker.gpu": {
                    "min_workers": 0,
                    "max_workers": 0,  # No GPU nodes
                }
            },
        })
        self.cluster.start()
        self.cluster.connect(client=True, timeout=120)

        def autoscaling_train(config):
            time.sleep(120)
            tune.report(1.)

        tune.run(
            autoscaling_train,
            num_samples=3,
            resources_per_trial={"cpu": 4},
            fail_fast=True)

    def testFaultTolerance(self):
        """Test that Tune run can recover from a failed node.

        When `max_failures` is set to larger than zero.
        """

        self.cluster.update_config({
            "provider": {
                "head_resources": {
                    "CPU": 4,
                    "GPU": 0
                }
            },
            "available_node_types": {
                "ray.worker.cpu": {
                    "resources": {
                        "CPU": 4
                    },
                    "min_workers": 0,  # No minimum nodes
                    "max_workers": 2,
                },
                "ray.worker.gpu": {
                    "min_workers": 0,
                    "max_workers": 0,  # No GPU nodes
                }
            },
        })
        self.cluster.start()
        self.cluster.connect(client=True, timeout=120)

        def train(config):
            time.sleep(120)
            tune.report(1.)

        class FailureInjectionCallback(Callback):
            def __init__(self, cluster):
                self._cluster = cluster
                self._killed = False

            def on_step_begin(self, iteration, trials, **info):
                if not self._killed and len(trials) == 3 and all(
                        trial.status == Trial.RUNNING for trial in trials):
                    self._cluster.kill_node(num=2)
                    self._killed = True

        tune.run(
            train,
            num_samples=3,
            resources_per_trial={"cpu": 4},
            max_failures=1,
            callbacks=[FailureInjectionCallback(self.cluster)],
            # The following two are to be removed once we have proper setup
            # for killing nodes while in ray client mode.
            _remote=False,
            local_dir="/tmp/ray_results/",
        )


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main(["-v", __file__]))

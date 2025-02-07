import sys
import time
import unittest

import ray
from ray import train, tune
from ray.air.util.node import _force_on_node
from ray.autoscaler._private.fake_multi_node.test_utils import DockerCluster
from ray.tune.callback import Callback
from ray.tune.experiment import Trial
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy


@ray.remote
def remote_task(val):
    return val


class MultiNodeSyncTest(unittest.TestCase):
    def setUp(self):
        self.cluster = DockerCluster()
        self.cluster.setup()

    def tearDown(self):
        self.cluster.stop()
        self.cluster.teardown(keep_dir=True)

    def testClusterAutoscaling(self):
        """Sanity check that multinode tests with autoscaling are working"""
        self.cluster.update_config(
            {
                "provider": {"head_resources": {"CPU": 4, "GPU": 0}},
            }
        )
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
                    num_cpus=1,
                    num_gpus=1,
                    scheduling_strategy=PlacementGroupSchedulingStrategy(
                        placement_group=pg
                    ),
                ).remote(5)
            ),
        )

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
                raise RuntimeError("Re-starting killed node failed or too slow.")
            time.sleep(1)
            table = ray.util.placement_group_table(pg)

        print("Node was restarted.")

    def testAutoscalingNewNode(self):
        """Test that newly added nodes from autoscaling are not stale."""
        self.cluster.update_config(
            {
                "provider": {"head_resources": {"CPU": 2, "GPU": 0}},
                "available_node_types": {
                    "ray.worker.cpu": {
                        "resources": {"CPU": 4},
                        "min_workers": 0,  # No minimum nodes
                        "max_workers": 2,
                    },
                    "ray.worker.gpu": {
                        "min_workers": 0,
                        "max_workers": 0,  # No GPU nodes
                    },
                },
            }
        )
        self.cluster.start()
        self.cluster.connect(client=True, timeout=120)

        def autoscaling_train(config):
            time.sleep(120)
            train.report({"_metric": 1.0})

        tune.run(
            autoscaling_train,
            num_samples=3,
            resources_per_trial={"cpu": 4},
            fail_fast=True,
        )

    def testFaultTolerance(self):
        """Test that Tune run can recover from a failed node.

        When `max_failures` is set to larger than zero.
        """
        self.cluster.update_config(
            {
                "provider": {"head_resources": {"CPU": 2, "GPU": 0}},
                "available_node_types": {
                    "ray.worker.cpu": {
                        "resources": {"CPU": 4},
                        "min_workers": 0,  # No minimum nodes
                        "max_workers": 2,
                    },
                    "ray.worker.gpu": {
                        "min_workers": 0,
                        "max_workers": 0,  # No GPU nodes
                    },
                },
            }
        )
        self.cluster.start()
        self.cluster.connect(client=True, timeout=120)
        remote_api = self.cluster.remote_execution_api()

        def train_fn(config):
            time.sleep(120)
            train.report({"_metric": 1.0})

        class FailureInjectionCallback(Callback):
            def __init__(self):
                self._killed = False

            def on_step_begin(self, iteration, trials, **info):
                if (
                    not self._killed
                    and len(trials) == 3
                    and all(trial.status == Trial.RUNNING for trial in trials)
                ):
                    remote_api.kill_node(num=2)
                    self._killed = True

        tune.run(
            train_fn,
            num_samples=3,
            resources_per_trial={"cpu": 4},
            max_failures=1,
            callbacks=[FailureInjectionCallback()],
        )

    def testForceOnNodeScheduling(self):
        """Test node scheduling behavior correctly schedules with node affinity."""
        num_workers = 4
        num_cpu_per_node = 4
        self.cluster.update_config(
            {
                "provider": {"head_resources": {"CPU": num_cpu_per_node, "GPU": 0}},
                "available_node_types": {
                    "ray.worker.cpu": {
                        "resources": {"CPU": num_cpu_per_node},
                        "min_workers": num_workers,
                        "max_workers": num_workers,
                    },
                    "ray.worker.gpu": {
                        "min_workers": 0,
                        "max_workers": 0,  # No GPU nodes
                    },
                },
            }
        )
        self.cluster.start()
        self.cluster.connect(client=True, timeout=120)

        total_num_cpu = (1 + num_workers) * num_cpu_per_node
        self.cluster.wait_for_resources({"CPU": total_num_cpu})

        @ray.remote
        def get_current_node_id():
            return ray.get_runtime_context().get_node_id()

        node_ids = [node["NodeID"] for node in ray.nodes()]
        assert len(node_ids) == 1 + num_workers

        remote_tasks = [
            _force_on_node(node_id, get_current_node_id).remote()
            for node_id in node_ids
        ]
        results = ray.get(remote_tasks)
        print(results)
        assert results == node_ids


if __name__ == "__main__":
    import pytest

    sys.exit(pytest.main(["-v", __file__]))

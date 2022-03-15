import json
import os
import sys
import time
import unittest
from typing import List

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
                remote_task.options(num_cpus=1, num_gpus=1, placement_group=pg).remote(
                    5
                )
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
                "provider": {"head_resources": {"CPU": 4, "GPU": 0}},
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
            tune.report(1.0)

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
                "provider": {"head_resources": {"CPU": 4, "GPU": 0}},
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

        def train(config):
            time.sleep(120)
            tune.report(1.0)

        class FailureInjectionCallback(Callback):
            def __init__(self, cluster):
                self._cluster = cluster
                self._killed = False

            def on_step_begin(self, iteration, trials, **info):
                if (
                    not self._killed
                    and len(trials) == 3
                    and all(trial.status == Trial.RUNNING for trial in trials)
                ):
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

    def testCheckpointSync(self):
        """Test that checkpoints are correctly synced."""

        self.cluster.update_config(
            {
                "provider": {"head_resources": {"CPU": 4, "GPU": 0}},
                "available_node_types": {
                    "ray.worker.cpu": {
                        "resources": {"CPU": 4},
                        "min_workers": 2,
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
        self.cluster.wait_for_resources({"CPU": 12})

        def train(config, checkpoint_dir=None):
            start = 0
            if checkpoint_dir:
                with open(os.path.join(checkpoint_dir, "checkpoint.json"), "rt") as fp:
                    state = json.load(fp)
                    start = state["step"] + 1

            for i in range(start, start + 10):
                with tune.checkpoint_dir(i) as d:
                    with open(os.path.join(d, "checkpoint.json"), "wt") as fp:
                        json.dump({"step": i}, fp)
                tune.report(step=i)

            time.sleep(6)

            if start == 0:
                raise RuntimeError("First time we fail.")

        analysis = tune.run(
            train,
            name="checkpoint_test",
            num_samples=3,
            resources_per_trial={"cpu": 4},
            max_failures=0,
            local_dir="/cluster/node",
            trial_name_creator=lambda trial: trial.trial_id,
            trial_dirname_creator=lambda trial: trial.trial_id,
            keep_checkpoints_num=2,
            raise_on_failed_trial=False,
        )

        nodes_dir = os.path.join(self.cluster.cluster_dir, "nodes")

        def get_trial_path(ip_to_trial_dir, node) -> str:
            node_id = node["NodeID"]
            node_ip = node["NodeManagerAddress"]
            node_trial_id = ip_to_trial_dir[node_ip]

            node_dir = os.path.join(nodes_dir, node_id)

            node_trial_dir = os.path.join(node_dir, "checkpoint_test", node_trial_id)
            return node_trial_dir

        def get_checkpoint_dirs(node_trial_path: str) -> List[str]:
            return [
                path
                for path in os.listdir(node_trial_path)
                if path.startswith("checkpoint_")
            ]

        ip_to_trial_dir = {
            trial.last_result["node_ip"]: trial.trial_id for trial in analysis.trials
        }

        for node in ray.nodes():
            node_trial_dir = get_trial_path(ip_to_trial_dir, node)
            checkpoint_dirs = get_checkpoint_dirs(node_trial_dir)

            self.assertSequenceEqual(
                sorted(checkpoint_dirs), ["checkpoint_000008", "checkpoint_000009"]
            )

        class UpdateMaxFailures(Callback):
            def __init__(self, num_failures: int = 0):
                self._num_failures = num_failures
                self._initialized = False

            def on_step_begin(self, iteration, trials, **info):
                if not self._initialized:
                    for trial in trials:
                        trial.max_failures = self._num_failures
                    self._initialized = False

        analysis = tune.run(
            train,
            name="checkpoint_test",
            resources_per_trial={"cpu": 4},
            max_failures=0,
            local_dir="/cluster/node",
            trial_name_creator=lambda trial: trial.trial_id,
            trial_dirname_creator=lambda trial: trial.trial_id,
            keep_checkpoints_num=2,
            resume="ERRORED_ONLY",
            callbacks=[UpdateMaxFailures(num_failures=1)],
            fail_fast=True,
        )

        ip_to_trial_dir = {
            trial.last_result["node_ip"]: trial.trial_id for trial in analysis.trials
        }

        for node in ray.nodes():
            node_trial_dir = get_trial_path(ip_to_trial_dir, node)
            checkpoint_dirs = get_checkpoint_dirs(node_trial_dir)

            print(node_trial_dir, checkpoint_dirs)


if __name__ == "__main__":
    import pytest

    sys.exit(pytest.main(["-v", __file__]))

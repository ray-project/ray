import json
import os
import sys
import time
import unittest
from typing import List

import ray
from ray import tune
from ray.autoscaler._private.fake_multi_node.node_provider import FAKE_HEAD_NODE_ID
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
        """Test that checkpoints are correctly synced.

        This test starts a three node cluster and runs three trials on them,
        all saving 10 checkpoints. Two checkpoints are kept per trial.
        This functionality is asserted by this test.

        The experiment is interrupted and restarted. We then continue to train
        another 10 rounds. We assert that the trials continue to train from
        their checkpoints, and that at most 4 checkpoints are kept
        per trial (2 from the first trial, 2 from the second).

        In the future, we may want to revise this behavior to only keep
        2 checkpoints at any time, even when continuing from an existing
        experiment.
        """

        # This cluster config starts 3 nodes a 4 CPUs
        self.cluster.update_config(
            {
                "provider": {
                    "head_resources": {"CPU": 4, "GPU": 0},
                    "env_vars": {"TUNE_GLOBAL_CHECKPOINT_S": "0"},
                },
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
        # Connect via Ray client and wait until all nodes are there
        self.cluster.start()
        self.cluster.connect(client=True, timeout=120)
        self.cluster.wait_for_resources({"CPU": 12})

        # This train function trains for 10 iterations per run
        def train(config, checkpoint_dir=None):
            start = i = 0
            if checkpoint_dir:
                with open(os.path.join(checkpoint_dir, "checkpoint.json"), "rt") as fp:
                    state = json.load(fp)
                    start = state["step"] + 1

            for i in range(start, start + 10):
                with tune.checkpoint_dir(i) as d:
                    with open(os.path.join(d, "checkpoint.json"), "wt") as fp:
                        json.dump({"step": i}, fp)
                tune.report(step=i)

            # These indicators will tell us if all trials saved their
            # checkpoints.
            with open(f"/cluster/shared/indicator.{tune.get_trial_id()}", "wt") as fp:
                fp.write("")

            # We continue training (without saving checkpoints) to make sure
            # that Tune's result handling is triggered (so that the
            # FailOnIndicator callback is invoked).
            time.sleep(6)
            tune.report(step=i + 1)
            time.sleep(6)

            if start == 0:
                # If this is the first round, we just sleep for some time
                # to make sure that the driver exits first (via the
                # FailOnIndicator)
                tune.report(step=i + 2)
                time.sleep(120)

        # This is a callback that checks if all indicators are there
        # (written by the training functions). If so, the tune run
        # should end (by raising an error).
        class FailOnIndicator(Callback):
            def __init__(self, indicator_dir: str, num_indicators: int = 3):
                self._indicator_dir = indicator_dir
                self._num_indicators = num_indicators

            def on_step_begin(self, iteration, trials, **info):
                if os.path.exists(self._indicator_dir):
                    indicators = [
                        f
                        for f in os.listdir(self._indicator_dir)
                        if f.startswith("indicator")
                    ]
                    if len(indicators) >= self._num_indicators:
                        raise RuntimeError("All the indicators are there.")

        # Run our test
        with self.assertRaises(RuntimeError):
            tune.run(
                train,
                name="checkpoint_test",
                num_samples=3,
                resources_per_trial={"cpu": 4},
                max_failures=0,
                local_dir="/cluster/node",
                trial_name_creator=lambda trial: trial.trial_id,
                trial_dirname_creator=lambda trial: trial.trial_id,
                keep_checkpoints_num=2,
                callbacks=[FailOnIndicator("/cluster/shared", num_indicators=3)],
                verbose=2,
            )

        nodes_dir = os.path.join(self.cluster.cluster_dir, "nodes")

        # Helper functions to analyze the experiment directory post-experiment
        def get_trial_path(ip_to_trial_dir: dict, node: dict) -> str:
            """Return path on the host dir for a specific trial.

            This is the directory the trial writes it's checkpoints to.
            """
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

        # Since the tune run failed, load our own experiment analysis here
        analysis = tune.ExperimentAnalysis(
            os.path.join(nodes_dir, FAKE_HEAD_NODE_ID, "checkpoint_test")
        )
        # Map IP: trial_id
        ip_to_trial_dir = {
            trial.last_result["node_ip"]: trial.trial_id for trial in analysis.trials
        }

        # Each node will contain 1 trial
        for node in ray.nodes():
            node_trial_dir = get_trial_path(ip_to_trial_dir, node)
            checkpoint_dirs = get_checkpoint_dirs(node_trial_dir)

            # Each trial should have saved two checkpoints
            self.assertSequenceEqual(
                sorted(checkpoint_dirs), ["checkpoint_000008", "checkpoint_000009"]
            )

        # Continue running the experiment in a new run. Note we
        # don't invoke any failure callback here.
        analysis = tune.run(
            train,
            name="checkpoint_test",
            resources_per_trial={"cpu": 4},
            local_dir="/cluster/node",
            keep_checkpoints_num=2,
            resume="AUTO",
            verbose=2,
        )

        # Trials could have been scheduled on other nodes, so reconstruct
        # this map
        ip_to_trial_dir = {
            trial.last_result["node_ip"]: trial.trial_id for trial in analysis.trials
        }

        for node in ray.nodes():
            node_trial_dir = get_trial_path(ip_to_trial_dir, node)
            checkpoint_dirs = get_checkpoint_dirs(node_trial_dir)

            # Assert that checkpoint 18 and 19 are there, but not 17
            # (because we only keep 2)
            self.assertNotIn("checkpoint_000017", checkpoint_dirs)
            self.assertIn("checkpoint_000018", checkpoint_dirs)
            self.assertIn("checkpoint_000019", checkpoint_dirs)

            # Maximum should be 4 because the first trial creates
            # 2, and we currently don't delete these on continue
            self.assertLessEqual(len(checkpoint_dirs), 4)


if __name__ == "__main__":
    import pytest

    sys.exit(pytest.main(["-v", __file__]))

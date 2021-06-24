import os
import unittest

import ray
from ray import tune
from ray.tune import Callback
from ray.rllib.agents.pg import PGTrainer, DEFAULT_CONFIG
from ray.tune.ray_trial_executor import RayTrialExecutor
from ray.tune.trial import Trial
from ray.tune.utils.placement_groups import PlacementGroupFactory
from ray.util import placement_group_table

trial_executor = None


class _TestCallback(Callback):
    def on_step_end(self, iteration, trials, **info):
        num_finished = len([
            t for t in trials
            if t.status == Trial.TERMINATED or t.status == Trial.ERROR
        ])
        num_running = len([t for t in trials if t.status == Trial.RUNNING])

        num_staging = sum(
            len(s) for s in trial_executor._pg_manager._staging.values())
        num_ready = sum(
            len(s) for s in trial_executor._pg_manager._ready.values())
        num_in_use = len(trial_executor._pg_manager._in_use_pgs)
        num_cached = len(trial_executor._pg_manager._cached_pgs)

        total_num_tracked = num_staging + num_ready + \
            num_in_use + num_cached

        num_non_removed_pgs = len([
            p for pid, p in placement_group_table().items()
            if p["state"] != "REMOVED"
        ])
        num_removal_scheduled_pgs = len(
            trial_executor._pg_manager._pgs_for_removal)

        # All 3 trials (3 different learning rates) should be scheduled.
        assert 3 == min(3, len(trials))
        # Cannot run more than 2 at a time
        # (due to different resource restrictions in the test cases).
        assert num_running <= 2
        # The number of placement groups should decrease
        # when trials finish.
        assert max(3, len(trials)) - num_finished == total_num_tracked
        # The number of actual placement groups should match this.
        assert max(3, len(trials)) - num_finished == \
            num_non_removed_pgs - num_removal_scheduled_pgs


class TestPlacementGroups(unittest.TestCase):
    def setUp(self) -> None:
        os.environ["TUNE_PLACEMENT_GROUP_RECON_INTERVAL"] = "0"
        ray.init(num_cpus=6)

    def tearDown(self) -> None:
        ray.shutdown()

    def test_overriding_default_resource_request(self):
        config = DEFAULT_CONFIG.copy()
        config["model"]["fcnet_hiddens"] = [10]
        config["num_workers"] = 2
        # 3 Trials: Can only run 2 at a time (num_cpus=6; needed: 3).
        config["lr"] = tune.grid_search([0.1, 0.01, 0.001])
        config["env"] = "CartPole-v0"
        config["framework"] = "tf"

        class DefaultResourceRequest:
            @classmethod
            def default_resource_request(cls, config):
                head_bundle = {"CPU": 1, "GPU": 0}
                child_bundle = {"CPU": 1}
                return PlacementGroupFactory(
                    [head_bundle, child_bundle, child_bundle],
                    strategy=config["placement_strategy"])

        # Create a trainer with an overridden default_resource_request
        # method that returns a PlacementGroupFactory.
        MyTrainer = PGTrainer.with_updates(mixins=[DefaultResourceRequest])
        tune.register_trainable("my_trainable", MyTrainer)

        global trial_executor
        trial_executor = RayTrialExecutor(reuse_actors=False)

        tune.run(
            "my_trainable",
            config=config,
            stop={"training_iteration": 2},
            trial_executor=trial_executor,
            callbacks=[_TestCallback()],
            verbose=2,
        )

    def test_default_resource_request(self):
        config = DEFAULT_CONFIG.copy()
        config["model"]["fcnet_hiddens"] = [10]
        config["num_workers"] = 2
        config["num_cpus_per_worker"] = 2
        # 3 Trials: Can only run 1 at a time (num_cpus=6; needed: 5).
        config["lr"] = tune.grid_search([0.1, 0.01, 0.001])
        config["env"] = "CartPole-v0"
        config["framework"] = "torch"
        config["placement_strategy"] = "SPREAD"

        global trial_executor
        trial_executor = RayTrialExecutor(reuse_actors=False)

        tune.run(
            "PG",
            config=config,
            stop={"training_iteration": 2},
            trial_executor=trial_executor,
            callbacks=[_TestCallback()],
            verbose=2,
        )

    def test_default_resource_request_plus_manual_leads_to_error(self):
        config = DEFAULT_CONFIG.copy()
        config["model"]["fcnet_hiddens"] = [10]
        config["num_workers"] = 0
        config["env"] = "CartPole-v0"

        try:
            tune.run(
                "PG",
                config=config,
                stop={"training_iteration": 2},
                resources_per_trial=PlacementGroupFactory([{
                    "CPU": 1
                }]),
                verbose=2,
            )
        except ValueError as e:
            assert "have been automatically set to" in e.args[0]


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))

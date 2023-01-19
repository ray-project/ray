import os
import unittest

import ray
from ray import air
from ray import tune
from ray.tune import Callback
from ray.rllib.algorithms.pg import PG, DEFAULT_CONFIG
from ray.tune.experiment import Trial
from ray.tune.execution.placement_groups import PlacementGroupFactory

trial_executor = None


class _TestCallback(Callback):
    def on_step_end(self, iteration, trials, **info):
        num_running = len([t for t in trials if t.status == Trial.RUNNING])

        # All 3 trials (3 different learning rates) should be scheduled.
        assert 3 == min(3, len(trials))
        # Cannot run more than 2 at a time
        # (due to different resource restrictions in the test cases).
        assert num_running <= 2


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
        config["env"] = "CartPole-v1"
        config["framework"] = "tf"

        # Create an Algorithm with an overridden default_resource_request
        # method that returns a PlacementGroupFactory.

        class MyAlgo(PG):
            @classmethod
            def default_resource_request(cls, config):
                head_bundle = {"CPU": 1, "GPU": 0}
                child_bundle = {"CPU": 1}
                return PlacementGroupFactory(
                    [head_bundle, child_bundle, child_bundle],
                    strategy=config["placement_strategy"],
                )

        tune.register_trainable("my_trainable", MyAlgo)

        tune.Tuner(
            "my_trainable",
            param_space=config,
            run_config=air.RunConfig(
                stop={"training_iteration": 2},
                verbose=2,
                callbacks=[_TestCallback()],
            ),
        ).fit()

    def test_default_resource_request(self):
        config = DEFAULT_CONFIG.copy()
        config["model"]["fcnet_hiddens"] = [10]
        config["num_workers"] = 2
        config["num_cpus_per_worker"] = 2
        # 3 Trials: Can only run 1 at a time (num_cpus=6; needed: 5).
        config["lr"] = tune.grid_search([0.1, 0.01, 0.001])
        config["env"] = "CartPole-v1"
        config["framework"] = "torch"
        config["placement_strategy"] = "SPREAD"

        tune.Tuner(
            PG,
            param_space=config,
            run_config=air.RunConfig(
                stop={"training_iteration": 2},
                verbose=2,
                callbacks=[_TestCallback()],
            ),
            tune_config=tune.TuneConfig(reuse_actors=False),
        ).fit()

    def test_default_resource_request_plus_manual_leads_to_error(self):
        config = DEFAULT_CONFIG.copy()
        config["model"]["fcnet_hiddens"] = [10]
        config["num_workers"] = 0
        config["env"] = "CartPole-v1"

        try:
            tune.Tuner(
                tune.with_resources(PG, PlacementGroupFactory([{"CPU": 1}])),
                param_space=config,
                run_config=air.RunConfig(stop={"training_iteration": 2}, verbose=2),
            ).fit()
        except ValueError as e:
            assert "have been automatically set to" in e.args[0]


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))

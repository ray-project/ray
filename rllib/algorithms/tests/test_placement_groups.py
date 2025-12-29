import os
import unittest

import ray
from ray import tune
from ray.rllib.algorithms.ppo import PPO, PPOConfig
from ray.tune import Callback
from ray.tune.execution.placement_groups import PlacementGroupFactory
from ray.tune.experiment import Trial
from ray.tune.result import TRAINING_ITERATION

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
        # 3 Trials: Can only run 2 at a time (num_cpus=6; needed: 3).
        config = (
            PPOConfig()
            .api_stack(
                enable_env_runner_and_connector_v2=False,
                enable_rl_module_and_learner=False,
            )
            .training(
                model={"fcnet_hiddens": [10]}, lr=tune.grid_search([0.1, 0.01, 0.001])
            )
            .environment("CartPole-v1")
            .env_runners(num_env_runners=2)
            .framework("tf")
        )

        # Create an Algorithm with an overridden default_resource_request
        # method that returns a PlacementGroupFactory.

        class MyAlgo(PPO):
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
            run_config=tune.RunConfig(
                stop={TRAINING_ITERATION: 2},
                verbose=2,
                callbacks=[_TestCallback()],
            ),
        ).fit()

    def test_default_resource_request(self):
        config = (
            PPOConfig()
            .api_stack(
                enable_env_runner_and_connector_v2=False,
                enable_rl_module_and_learner=False,
            )
            .resources(placement_strategy="SPREAD")
            .env_runners(
                num_env_runners=2,
                num_cpus_per_env_runner=2,
            )
            .training(
                model={"fcnet_hiddens": [10]}, lr=tune.grid_search([0.1, 0.01, 0.001])
            )
            .environment("CartPole-v1")
            .framework("torch")
        )
        # 3 Trials: Can only run 1 at a time (num_cpus=6; needed: 5).

        tune.Tuner(
            PPO,
            param_space=config,
            run_config=tune.RunConfig(
                stop={TRAINING_ITERATION: 2},
                verbose=2,
                callbacks=[_TestCallback()],
            ),
            tune_config=tune.TuneConfig(reuse_actors=False),
        ).fit()

    def test_default_resource_request_plus_manual_leads_to_error(self):
        config = (
            PPOConfig()
            .api_stack(
                enable_env_runner_and_connector_v2=False,
                enable_rl_module_and_learner=False,
            )
            .training(model={"fcnet_hiddens": [10]})
            .environment("CartPole-v1")
            .env_runners(num_env_runners=0)
        )

        try:
            tune.Tuner(
                tune.with_resources(PPO, PlacementGroupFactory([{"CPU": 1}])),
                param_space=config,
                run_config=tune.RunConfig(stop={TRAINING_ITERATION: 2}, verbose=2),
            ).fit()
        except ValueError as e:
            assert "have been automatically set to" in e.args[0]


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))

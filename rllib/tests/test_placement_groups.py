import unittest

import ray
from ray import tune
from ray.rllib.agents.pg import PGTrainer, DEFAULT_CONFIG
from ray.tune.utils.placement_groups import PlacementGroupFactory


class TestPlacementGroups(unittest.TestCase):
    def setUp(self) -> None:
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

        tune.run(
            "my_trainable",
            config=config,
            stop={"training_iteration": 3},
            verbose=2,
        )

    def test_default_resource_request(self):
        config = DEFAULT_CONFIG.copy()
        config["model"]["fcnet_hiddens"] = [10]
        config["num_workers"] = 2
        config["num_cpus_per_worker"] = 1
        # 3 Trials: Can only run 1 at a time (num_cpus=6; needed: 5).
        config["lr"] = tune.grid_search([0.1, 0.01, 0.001])
        config["env"] = "CartPole-v0"
        config["framework"] = "torch"
        config["placement_strategy"] = "SPREAD"

        tune.run(
            "PG",
            config=config,
            stop={"training_iteration": 3},
            verbose=2,
        )


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))

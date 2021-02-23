import unittest

import ray
from ray import tune
from ray.rllib.agents.pg import PGTrainer, DEFAULT_CONFIG
from ray.tune.utils.placement_groups import PlacementGroupFactory
from ray.util import placement_group


class TestPlacementGroups(unittest.TestCase):
    def setUp(self) -> None:
        ray.init(num_cpus=6)

    def tearDown(self) -> None:
        ray.shutdown()

    def test_placement_groups(self):
        config = DEFAULT_CONFIG.copy()
        config["model"]["fcnet_hiddens"] = [10]
        config["num_workers"] = 2
        # 3 Trials: Can only run 2 at a time (num_cpus=6; needed: 3).
        config["lr"] = tune.grid_search([0.1, 0.01, 0.001])
        config["env"] = "CartPole-v0"
        config["framework"] = "torch"

        def pg_factory():
            head_bundle = {"CPU": 1, "GPU": 0}
            child_bundle = {"CPU": 1}
            return placement_group([head_bundle, child_bundle, child_bundle])

        class DefaultResourceRequest:
            @classmethod
            def default_resource_request(cls, config):
                from ray.tune.resources import Resources
                return Resources(cpu=10, gpu=0)

        MyTrainer = PGTrainer.with_updates(mixins=[DefaultResourceRequest])
        tune.register_trainable("my_trainable", MyTrainer)

        tune.run(
            "my_trainable", config=config, stop={"training_iteration": 1},
            #resources_per_trial=pg_factory,
        )


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))

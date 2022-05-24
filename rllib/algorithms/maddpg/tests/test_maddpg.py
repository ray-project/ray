import unittest

import ray
import ray.rllib.algorithms.maddpg as maddpg
from ray.rllib.examples.env.two_step_game import TwoStepGame
from ray.rllib.policy.policy import PolicySpec
from ray.rllib.utils.test_utils import (
    check_train_results,
    framework_iterator,
)


class TestMADDPG(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_maddpg_compilation(self):
        """Test whether an MADDPGTrainer can be built with all frameworks."""
        config = maddpg.DEFAULT_CONFIG.copy()
        config["env"] = TwoStepGame
        config["env_config"] = {
            "actions_are_logits": True,
        }
        config["multiagent"] = {
            "policies": {
                "pol1": PolicySpec(
                    config={"agent_id": 0},
                ),
                "pol2": PolicySpec(
                    config={"agent_id": 1},
                ),
            },
            "policy_mapping_fn": (lambda aid, **kwargs: "pol2" if aid else "pol1"),
        }

        num_iterations = 1

        # Only working for tf right now.
        for _ in framework_iterator(config, frameworks="tf"):
            trainer = maddpg.MADDPGTrainer(config)
            for i in range(num_iterations):
                results = trainer.train()
                check_train_results(results)
                print(results)
            trainer.stop()


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))

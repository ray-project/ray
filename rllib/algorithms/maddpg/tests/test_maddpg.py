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
        """Test whether MADDPG can be built with all frameworks."""
        config = (
            maddpg.MADDPGConfig()
            .environment(
                env=TwoStepGame,
                env_config={
                    "actions_are_logits": True,
                },
            )
            .multi_agent(
                policies={
                    "pol1": PolicySpec(
                        config={"agent_id": 0},
                    ),
                    "pol2": PolicySpec(
                        config={"agent_id": 1},
                    ),
                },
                policy_mapping_fn=lambda agent_id, **kwargs: "pol2"
                if agent_id
                else "pol1",
            )
        )

        num_iterations = 1

        # Only working for tf right now.
        for _ in framework_iterator(config, frameworks="tf"):
            algo = config.build()
            for i in range(num_iterations):
                results = algo.train()
                check_train_results(results)
                print(results)
            algo.stop()


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))

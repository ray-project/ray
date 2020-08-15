import numpy as np
import ray
import sys
import unittest

from ray.rllib.utils import check
import ray.rllib.agents.ppo as ppo


class TestCuriosity(unittest.TestCase):

    # Sets up a single ray environment for every test.

    @classmethod
    def setUpClass(cls):
        ray.init(local_mode=True)

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_curiosity(self):
        config = ppo.DEFAULT_CONFIG

        env = "CartPole-v0"
        config["framework"] = "torch"
        config["exploration_config"] = {
            "type": "ray.rllib.utils.exploration.curiosity.Curiosity",
            "forward_net_hiddens": [64],
            "inverse_net_hiddens": [32, 4],
            "feature_net_hiddens": [16, 8],
            "feature_dim": 8,
            "forward_activation": "relu",
            "inverse_activation": "relu",
            "feature_activation": "relu",
            #"submodule": "EpsilonGreedy",
        }
        trainer = ppo.PPOTrainer(config=config, env=env)
        trainer.train()
        trainer.stop()


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main(["-v", __file__]))

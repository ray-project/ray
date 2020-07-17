import numpy as np
import ray
import sys
import unittest

from ray.rllib.utils.exploration import ParameterNoise
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

    def test_useless(self):
        print("RAN")

    def test_no_curiosity(self):
        config = ppo.DEFAULT_CONFIG
        env = "CartPole-v0"
        dummy_obs = np.array([0.0, 0.1, 0.0, 0.0])
        prev_a = np.array(0)

        config["exploration_config"] = {"type": "ParameterNoise"}

        trainer = ppo.PPOTrainer(config=config, env=env)
        trainer.train()

        # Make sure all actions drawn are the same, given same
        # observations.

        actions = []
        for _ in range(25):
            actions.append(
                trainer.compute_action(
                    observation=dummy_obs,
                    explore=False,
                    prev_action=prev_a,
                    prev_reward=1.0 if prev_a is not None else None))
            #check(actions[-1], actions[0])
        print(actions)
#        trainer.train()


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main(["-v", __file__]))

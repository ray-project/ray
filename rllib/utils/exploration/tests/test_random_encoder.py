import sys
import unittest

import pytest
import ray
import ray.rllib.algorithms.ppo as ppo
import ray.rllib.algorithms.sac as sac
from ray.rllib.algorithms.callbacks import RE3UpdateCallbacks


class TestRE3(unittest.TestCase):
    """Tests for RE3 exploration algorithm."""

    @classmethod
    def setUpClass(cls):
        ray.init()

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def run_re3(self, rl_algorithm):
        """Tests RE3 for PPO and SAC.

        Both the on-policy and off-policy setups are validated.
        """
        if rl_algorithm == "PPO":
            config = ppo.PPOConfig().to_dict()
            algo_cls = ppo.PPO
            beta_schedule = "constant"
        elif rl_algorithm == "SAC":
            config = sac.SACConfig().to_dict()
            algo_cls = sac.SAC
            beta_schedule = "linear_decay"

        class RE3Callbacks(RE3UpdateCallbacks, config["callbacks"]):
            pass

        config["env"] = "Pendulum-v1"
        config["callbacks"] = RE3Callbacks
        config["exploration_config"] = {
            "type": "RE3",
            "embeds_dim": 128,
            "beta_schedule": beta_schedule,
            "sub_exploration": {
                "type": "StochasticSampling",
            },
        }

        num_iterations = 30
        algo = algo_cls(config=config)
        learnt = False
        for i in range(num_iterations):
            result = algo.train()
            print(result)
            if result["episode_reward_max"] > -900.0:
                print("Reached goal after {} iters!".format(i))
                learnt = True
                break
        algo.stop()
        self.assertTrue(learnt)

    def test_re3_ppo(self):
        """Tests RE3 with PPO."""
        self.run_re3("PPO")

    def test_re3_sac(self):
        """Tests RE3 with SAC."""
        self.run_re3("SAC")


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))

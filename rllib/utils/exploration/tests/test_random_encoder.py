import sys
import unittest

import pytest
import ray
from ray.rllib.agents import ppo, sac
from ray.rllib.agents.callbacks import RE3UpdateCallbacks


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
            config = ppo.DEFAULT_CONFIG.copy()
            trainer_cls = ppo.PPOTrainer
            beta_schedule = "constant"
        elif rl_algorithm == "SAC":
            config = sac.DEFAULT_CONFIG.copy()
            trainer_cls = sac.SACTrainer
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
        trainer = trainer_cls(config=config)
        learnt = False
        for i in range(num_iterations):
            result = trainer.train()
            print(result)
            if result["episode_reward_max"] > -900.0:
                print("Reached goal after {} iters!".format(i))
                learnt = True
                break
        trainer.stop()
        self.assertTrue(learnt)

    def test_re3_ppo(self):
        """Tests RE3 with PPO."""
        self.run_re3("PPO")

    def test_re3_sac(self):
        """Tests RE3 with SAC."""
        self.run_re3("SAC")


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))

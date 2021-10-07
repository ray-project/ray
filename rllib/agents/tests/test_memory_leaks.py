import unittest

import ray
import ray.rllib.agents.dqn as dqn
import ray.rllib.agents.ppo as ppo
from ray.rllib.examples.env.memory_leaking_env import MemoryLeakingEnv
from ray.rllib.examples.policy.memory_leaking_policy import MemoryLeakingPolicy
from ray.rllib.policy.policy import PolicySpec
from ray.rllib.utils.debug.memory import check_memory_leaks


class TestMemoryLeaks(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init(local_mode=True)#TODO

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_leaky_env(self):
        config = ppo.DEFAULT_CONFIG.copy()
        # Make sure we have an env to test on the local worker.
        # Otherwise, `check_memory_leaks` will complain.
        config["create_env_on_driver"] = True
        config["env"] = MemoryLeakingEnv
        trainer = ppo.PPOTrainer(config=config)
        self.assertRaisesRegex(
            MemoryError, "Found a memory leak inside.+memory_leaking_env.py",
            lambda: check_memory_leaks(trainer, to_check={"env"})
        )
        trainer.stop()

    def test_leaky_policy(self):
        config = dqn.DEFAULT_CONFIG.copy()
        # Make sure we have an env to test on the local worker.
        # Otherwise, `check_memory_leaks` will complain.
        config["create_env_on_driver"] = True
        config["env"] = "CartPole-v0"
        config["multiagent"]["policies"] = {
            "default_policy": PolicySpec(policy_class=MemoryLeakingPolicy),
        }
        trainer = dqn.DQNTrainer(config=config)
        self.assertRaisesRegex(
            MemoryError, "Found a memory leak inside.+memory_leaking_policy.py",
            lambda: check_memory_leaks(trainer, to_check={"policy"})
        )
        trainer.stop()


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))

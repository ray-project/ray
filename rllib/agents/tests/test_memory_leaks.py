import unittest

import ray
import ray.rllib.agents.dqn as dqn
import ray.rllib.agents.ppo as ppo
from ray.rllib.examples.env.memory_leaking_env import MemoryLeakingEnv
from ray.rllib.examples.env.random_env import RandomEnv
from ray.rllib.examples.policy.memory_leaking_policy import MemoryLeakingPolicy
from ray.rllib.policy.policy import PolicySpec
from ray.rllib.utils.debug.memory import check_memory_leaks


class TestMemoryLeaks(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init()

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
        results = check_memory_leaks(trainer, to_check={"env"})
        assert results is not None
        trainer.stop()

        config["env"] = RandomEnv
        trainer = ppo.PPOTrainer(config=config)
        results = check_memory_leaks(trainer, to_check={"env"})
        assert results is None
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
        results = check_memory_leaks(trainer, to_check={"policy"})
        assert results is not None
        trainer.stop()


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))

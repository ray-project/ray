import unittest

import ray
import ray.rllib.algorithms.dqn as dqn
import ray.rllib.algorithms.ppo as ppo
from ray.rllib.examples.env.memory_leaking_env import MemoryLeakingEnv
from ray.rllib.examples.policy.memory_leaking_policy import MemoryLeakingPolicy
from ray.rllib.policy.policy import PolicySpec
from ray.rllib.utils.debug.memory import check_memory_leaks


class TestMemoryLeaks(unittest.TestCase):
    """Generically tests our memory leak diagnostics tools."""

    @classmethod
    def setUpClass(cls):
        ray.init()

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_leaky_env(self):
        """Tests, whether our diagnostics tools can detect leaks in an env."""
        config = (
            ppo.PPOConfig().environment(
                MemoryLeakingEnv, env_config={"static_samples": True}
            )
            # Make sure we have an env to test on the local worker.
            # Otherwise, `check_memory_leaks` will complain.
            .rollouts(create_env_on_local_worker=True)
        )
        algo = config.build()
        results = check_memory_leaks(algo, to_check={"env"}, repeats=150)
        assert results["env"]
        algo.stop()

    def test_leaky_policy(self):
        """Tests, whether our diagnostics tools can detect leaks in a policy."""
        config = (
            dqn.DQNConfig()
            .environment("CartPole-v1")
            # Make sure we have an env to test on the local worker.
            # Otherwise, `check_memory_leaks` will complain.
            .rollouts(create_env_on_local_worker=True)
            .multi_agent(
                policies={
                    "default_policy": PolicySpec(policy_class=MemoryLeakingPolicy),
                }
            )
        )
        algo = config.build()
        results = check_memory_leaks(algo, to_check={"policy"}, repeats=300)
        assert results["policy"]
        algo.stop()


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))

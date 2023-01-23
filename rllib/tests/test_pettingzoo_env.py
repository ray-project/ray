import gymnasium as gym
from numpy import float32
from pettingzoo.butterfly import pistonball_v6
from pettingzoo.mpe import simple_spread_v2
from supersuit import normalize_obs_v0, dtype_v0, color_reduction_v0
import unittest

import ray
from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.env import PettingZooEnv


# TODO(sven): Move into rllib/env/wrappers/tests/.
class TestPettingZooEnv(unittest.TestCase):
    def setUp(self) -> None:
        ray.init()

    def tearDown(self) -> None:
        ray.shutdown()

    def test_pettingzoo_pistonball_v6_policies_are_dict_env(self):
        def env_creator():
            env = pistonball_v6.env()
            env = dtype_v0(env, dtype=float32)
            env = color_reduction_v0(env, mode="R")
            env = normalize_obs_v0(env)
            return env

        # Register env
        gym.register("pistonball", lambda: PettingZooEnv(env_creator()))

        config = (
            PPOConfig()
            .environment("pistonball")
            .multi_agent(
                # Set of policy IDs (by default, will use Algorithms's
                # default policy class, the env's/agent's obs/act spaces and config={}).
                policies={"av"},
                # Map all agents to that policy.
                policy_mapping_fn=lambda agent_id, episode, worker, **kwargs: "av",
            )
            .debugging(log_level="DEBUG")
            .rollouts(
                num_rollout_workers=1,
                # Fragment length, collected at once from each worker
                # and for each agent!
                rollout_fragment_length=30,
            )
            # Training batch size -> Fragments are concatenated up to this point.
            .training(train_batch_size=200)
        )

        algo = config.build()
        algo.train()
        algo.stop()

    def test_pettingzoo_env(self):
        gym.register("simple_spread", lambda: PettingZooEnv(simple_spread_v2.env()))

        config = (
            PPOConfig()
            .environment("simple_spread")
            .rollouts(num_rollout_workers=0, rollout_fragment_length=30)
            .debugging(log_level="DEBUG")
            .training(train_batch_size=200)
            .multi_agent(
                # Set of policy IDs (by default, will use Algorithm's
                # default policy class, the env's/agent's obs/act spaces and config={}).
                policies={"av"},
                # Mapping function that always returns "av" as policy ID to use
                # (for any agent).
                policy_mapping_fn=lambda agent_id, episode, worker, **kwargs: "av",
            )
        )

        algo = config.build()
        algo.train()
        algo.stop()


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))

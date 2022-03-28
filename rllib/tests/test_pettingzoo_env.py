import unittest
from copy import deepcopy
from numpy import float32

import ray
from ray.tune.registry import register_env
from ray.rllib.env import PettingZooEnv
from ray.rllib.agents.registry import get_trainer_class

from pettingzoo.butterfly import pistonball_v6
from pettingzoo.mpe import simple_spread_v2
from supersuit import normalize_obs_v0, dtype_v0, color_reduction_v0


class TestPettingZooEnv(unittest.TestCase):
    def setUp(self) -> None:
        ray.init()

    def tearDown(self) -> None:
        ray.shutdown()

    def test_pettingzoo_pistonball_v6_policies_are_dict_env(self):
        def env_creator(config):
            env = pistonball_v6.env()
            env = dtype_v0(env, dtype=float32)
            env = color_reduction_v0(env, mode="R")
            env = normalize_obs_v0(env)
            return env

        config = deepcopy(get_trainer_class("PPO").get_default_config())
        config["env_config"] = {"local_ratio": 0.5}
        # Register env
        register_env("pistonball", lambda config: PettingZooEnv(env_creator(config)))
        env = PettingZooEnv(env_creator(config))
        observation_space = env.observation_space
        action_space = env.action_space
        del env

        config["multiagent"] = {
            # Setup a single, shared policy for all agents.
            "policies": {"av": (None, observation_space, action_space, {})},
            # Map all agents to that policy.
            "policy_mapping_fn": lambda agent_id, episode, **kwargs: "av",
        }

        config["log_level"] = "DEBUG"
        config["num_workers"] = 1
        # Fragment length, collected at once from each worker
        # and for each agent!
        config["rollout_fragment_length"] = 30
        # Training batch size -> Fragments are concatenated up to this point.
        config["train_batch_size"] = 200
        # After n steps, force reset simulation
        config["horizon"] = 200
        # Default: False
        config["no_done_at_end"] = False
        trainer = get_trainer_class("PPO")(env="pistonball", config=config)
        trainer.train()

    def test_pettingzoo_env(self):
        register_env("simple_spread", lambda _: PettingZooEnv(simple_spread_v2.env()))
        env = PettingZooEnv(simple_spread_v2.env())
        observation_space = env.observation_space
        action_space = env.action_space
        del env

        agent_class = get_trainer_class("PPO")

        config = deepcopy(agent_class.get_default_config())

        config["multiagent"] = {
            # Set of policy IDs (by default, will use Trainer's
            # default policy class, the env's obs/act spaces and config={}).
            "policies": {"av": (None, observation_space, action_space, {})},
            # Mapping function that always returns "av" as policy ID to use
            # (for any agent).
            "policy_mapping_fn": lambda agent_id, episode, **kwargs: "av",
        }

        config["log_level"] = "DEBUG"
        config["num_workers"] = 0
        config["rollout_fragment_length"] = 30
        config["train_batch_size"] = 200
        config["horizon"] = 200  # After n steps, force reset simulation
        config["no_done_at_end"] = False

        agent = agent_class(env="simple_spread", config=config)
        agent.train()


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))

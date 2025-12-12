import unittest

from numpy import float32
from pettingzoo.butterfly import pistonball_v6
from pettingzoo.mpe import simple_spread_v3
from supersuit import (
    color_reduction_v0,
    dtype_v0,
    normalize_obs_v0,
    observation_lambda_v0,
    resize_v1,
)
from supersuit.utils.convert_box import convert_box

import ray
from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.env import PettingZooEnv
from ray.tune.registry import register_env


def change_observation(obs, obs_space):
    # convert all images to a 3d array with 1 channel
    obs = obs[..., None]
    return obs


def change_obs_space(obs_space):
    return convert_box(lambda obs: change_observation(obs, obs_space), obs_space)


# TODO(sven): Move into rllib/env/wrappers/tests/.
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
            # add a wrapper to convert the observation space to a 3d array
            env = observation_lambda_v0(env, change_observation, change_obs_space)
            # resize the observation space to 84x84 so that RLlib defauls CNN can
            # process it
            env = resize_v1(env, x_size=84, y_size=84, linear_interp=True)
            return env

        # Register env
        register_env("pistonball", lambda config: PettingZooEnv(env_creator(config)))

        config = (
            PPOConfig()
            .api_stack(
                enable_env_runner_and_connector_v2=False,
                enable_rl_module_and_learner=False,
            )
            .environment("pistonball", env_config={"local_ratio": 0.5})
            .multi_agent(
                # Set of policy IDs (by default, will use Algorithms's
                # default policy class, the env's/agent's obs/act spaces and config={}).
                policies={"av"},
                # Map all agents to that policy.
                policy_mapping_fn=lambda agent_id, episode, worker, **kwargs: "av",
            )
            .debugging(log_level="DEBUG")
            .env_runners(
                num_env_runners=1,
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
        register_env("simple_spread", lambda _: PettingZooEnv(simple_spread_v3.env()))

        config = (
            PPOConfig()
            .api_stack(
                enable_env_runner_and_connector_v2=False,
                enable_rl_module_and_learner=False,
            )
            .environment("simple_spread")
            .env_runners(num_env_runners=0, rollout_fragment_length=30)
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
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))

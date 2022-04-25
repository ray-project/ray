from gym.spaces import Box, Dict, Discrete, MultiDiscrete, Tuple
import numpy as np
import unittest

import ray
from ray.tune import register_env
from ray.rllib.agents.qmix import QMixTrainer
from ray.rllib.env.multi_agent_env import MultiAgentEnv


class AvailActionsTestEnv(MultiAgentEnv):
    num_actions = 10
    action_space = Discrete(num_actions)
    observation_space = Dict(
        {
            "obs": Dict(
                {
                    "test": Dict({"a": Discrete(2), "b": MultiDiscrete([2, 3, 4])}),
                    "state": MultiDiscrete([2, 2, 2]),
                }
            ),
            "action_mask": Box(0, 1, (num_actions,)),
        }
    )

    def __init__(self, env_config):
        super().__init__()
        self.state = None
        self.avail = env_config.get("avail_actions", [3])
        self.action_mask = np.array([0] * 10)
        for a in self.avail:
            self.action_mask[a] = 1

    def reset(self):
        self.state = 0
        return {
            "agent_1": {
                "obs": self.observation_space["obs"].sample(),
                "action_mask": self.action_mask,
            },
            "agent_2": {
                "obs": self.observation_space["obs"].sample(),
                "action_mask": self.action_mask,
            },
        }

    def step(self, action_dict):
        if self.state > 0:
            assert (
                action_dict["agent_1"] in self.avail
                and action_dict["agent_2"] in self.avail
            ), "Failed to obey available actions mask!"
        self.state += 1
        rewards = {"agent_1": 1, "agent_2": 0.5}
        obs = {
            "agent_1": {
                "obs": self.observation_space["obs"].sample(),
                "action_mask": self.action_mask,
            },
            "agent_2": {
                "obs": self.observation_space["obs"].sample(),
                "action_mask": self.action_mask,
            },
        }
        dones = {"__all__": self.state >= 20}
        return obs, rewards, dones, {}


class TestQMix(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init(local_mode=True)#TODO

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_avail_actions_qmix(self):
        grouping = {
            "group_1": ["agent_1", "agent_2"],
        }
        obs_space = Tuple(
            [
                AvailActionsTestEnv.observation_space,
                AvailActionsTestEnv.observation_space,
            ]
        )
        act_space = Tuple(
            [AvailActionsTestEnv.action_space, AvailActionsTestEnv.action_space]
        )
        register_env(
            "action_mask_test",
            lambda config: AvailActionsTestEnv(config).with_agent_groups(
                grouping, obs_space=obs_space, act_space=act_space
            ),
        )

        trainer = QMixTrainer(
            env="action_mask_test",
            config={
                "num_envs_per_worker": 5,  # test with vectorization on
                "env_config": {
                    "avail_actions": [3, 4, 8],
                },
                "framework": "torch",
            },
        )
        for _ in range(4):
            trainer.train()  # OK if it doesn't trip the action assertion error
        assert trainer.train()["episode_reward_mean"] == 30.0
        trainer.stop()
        ray.shutdown()

    #TODO: move this learning test into a yaml and add to weekly regression
    def test_qmix_on_pettingzoo_env(self):
        from ray.rllib.env.wrappers.pettingzoo_env import PettingZooEnv, ParallelPettingZooEnv
        from pettingzoo.butterfly import cooperative_pong_v4
        from gym.spaces import Tuple
        #from pettingzoo.butterfly import pistonball_v4
        #from pettingzoo.mpe import simple_spread_v2
        from supersuit import normalize_obs_v0, dtype_v0, color_reduction_v0, resize_v0, frame_stack_v1

        def env_creator(config):
            env = cooperative_pong_v4.parallel_env()
            env = dtype_v0(env, dtype=np.float32)
            env = resize_v0(env, 140, 240)
            env = color_reduction_v0(env, mode="R")
            env = frame_stack_v1(env, 4)
            env = normalize_obs_v0(env)
            return env

        env = ParallelPettingZooEnv(env_creator({}))
        observation_space = env.observation_space
        action_space = env.action_space
        del env

        # Group paddle0 and paddle1 into one single agent.
        grouping = {"paddles": ["paddle_0", "paddle_1"]}
        tuple_obs_space = Tuple([observation_space, observation_space])
        tuple_act_space = Tuple([action_space, action_space])

        register_env(
            "coop_pong",
            lambda config: ParallelPettingZooEnv(
                env_creator(config)
            ).with_agent_groups(
                grouping, obs_space=tuple_obs_space,
                act_space=tuple_act_space)
        )

        config = {
            "buffer_size": 10,
            "_disable_preprocessor_api": True,
        }
        trainer = QMixTrainer(env="coop_pong", config=config)
        trainer.train()



if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))

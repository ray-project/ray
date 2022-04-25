from gym.spaces import Tuple
import numpy as np
from pettingzoo.butterfly import cooperative_pong_v5
from supersuit import normalize_obs_v0, dtype_v0, color_reduction_v0, resize_v0, \
    frame_stack_v1

from ray.rllib.env.wrappers.pettingzoo_env import ParallelPettingZooEnv


def env_creator(config):
    env = cooperative_pong_v5.parallel_env()
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


def CooperativePong(env_ctx=None):
    return ParallelPettingZooEnv(
        env_creator(env_ctx or {})
    ).with_agent_groups(
        grouping, obs_space=tuple_obs_space,
        act_space=tuple_act_space)

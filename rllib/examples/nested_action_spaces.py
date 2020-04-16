import argparse
import gym
from gym.spaces import Dict, Tuple, Box, Discrete
import numpy as np
import tree

import ray
from ray import tune
from ray.tune.registry import register_env
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.space_utils import flatten_space

tf = try_import_tf()

parser = argparse.ArgumentParser()
parser.add_argument("--run", type=str, default="PPO")
parser.add_argument("--stop", type=int, default=90)
parser.add_argument("--num-cpus", type=int, default=0)


class NestedSpaceRepeatAfterMeEnv(gym.Env):
    """Env for which policy has to repeat the (possibly complex) observation.
    """

    def __init__(self, config):
        self.observation_space = config.get(
            "space", Tuple([Discrete(2),
                            Dict({
                                "a": Box(-1.0, 1.0, (2, ))
                            })]))
        self.action_space = self.observation_space
        self.flattened_action_space = flatten_space(self.action_space)
        self.episode_len = config.get("episode_len", 100)

    def reset(self):
        self.steps = 0
        return self._next_obs()

    def step(self, action):
        self.steps += 1
        action = tree.flatten(action)
        reward = 0.0
        for a, o, space in zip(action, self.current_obs_flattened,
                               self.flattened_action_space):
            # Box: -abs(diff).
            if isinstance(space, gym.spaces.Box):
                reward -= np.abs(np.sum(a - o))
            # Discrete: +1.0 if exact match.
            if isinstance(space, gym.spaces.Discrete):
                reward += 1.0 if a == o else 0.0
        done = self.steps >= self.episode_len
        return self._next_obs(), reward, done, {}

    def _next_obs(self):
        self.current_obs = self.observation_space.sample()
        self.current_obs_flattened = tree.flatten(self.current_obs)
        return self.current_obs


if __name__ == "__main__":
    args = parser.parse_args()
    ray.init(num_cpus=args.num_cpus or None)
    register_env("NestedSpaceRepeatAfterMeEnv",
                 lambda c: NestedSpaceRepeatAfterMeEnv(c))

    config = {
        "env": "NestedSpaceRepeatAfterMeEnv",
        "env_config": {
            "space": Dict({
                "a": Tuple([Dict({
                    "d": Box(0.0, 1.0, ()),
                    "e": Discrete(2)
                })]),
                "b": Box(-1.0, 1.0, (2, )),
                "c": Discrete(4)
            }),
        },
        "gamma": 0.0,  # No history in Env (bandit problem).
        "num_workers": 0,
        "num_envs_per_worker": 20,
        "entropy_coeff": 0.00005,  # We don't want high entropy in this Env.
        "num_sgd_iter": 20,
        "vf_loss_coeff": 0.01,
        "lr": 0.0005
    }

    tune.run(
        args.run,
        config=config,
        stop={"episode_reward_mean": args.stop},
        verbose=1)

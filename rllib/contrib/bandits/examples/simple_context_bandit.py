"""A very simple contextual bandit example with 3 arms."""

import argparse
import random
import numpy as np
import gym
from gym.spaces import Discrete, Box

from ray import tune

parser = argparse.ArgumentParser()
parser.add_argument("--stop-at-reward", type=float, default=10)
parser.add_argument("--run", type=str, default="contrib/LinUCB")


class SimpleContextualBandit(gym.Env):
    def __init__(self, config=None):
        self.action_space = Discrete(3)
        self.observation_space = Box(low=-1., high=1., shape=(2, ))
        self.cur_context = None

    def reset(self):
        self.cur_context = random.choice([-1., 1.])
        return np.array([self.cur_context, -self.cur_context])

    def step(self, action):
        rewards_for_context = {
            -1.: [-10, 0, 10],
            1.: [10, 0, -10],
        }
        reward = rewards_for_context[self.cur_context][action]
        return (np.array([-self.cur_context, self.cur_context]), reward, True,
                {
                    "regret": 10 - reward
                })


if __name__ == "__main__":
    args = parser.parse_args()
    tune.run(
        args.run,
        stop={
            "episode_reward_mean": args.stop_at_reward,
        },
        config={
            "env": SimpleContextualBandit,
        })

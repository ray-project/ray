"""A very simple contextual bandit example with 3 arms."""

import argparse
import gym
from gym.spaces import Discrete, Box
import numpy as np
import random

import ray
from ray import tune
from ray.rllib.utils.test_utils import check_learning_achieved

parser = argparse.ArgumentParser()
parser.add_argument("--run", type=str, default="contrib/LinUCB")
parser.add_argument("--as-test", action="store_true")
parser.add_argument("--stop-iters", type=int, default=200)
parser.add_argument("--stop-timesteps", type=int, default=100000)
parser.add_argument("--stop-reward", type=float, default=10.0)


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
    ray.init(num_cpus=3)
    args = parser.parse_args()

    stop = {
        "training_iteration": args.stop_iters,
        "timesteps_total": args.stop_timesteps,
        "episode_reward_mean": args.stop_reward,
    }

    config = {
        "env": SimpleContextualBandit,
    }

    results = tune.run(args.run, config=config, stop=stop)

    if args.as_test:
        check_learning_achieved(results, args.stop_reward)

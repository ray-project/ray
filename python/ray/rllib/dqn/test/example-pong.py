#!/usr/bin/env python

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse

import ray
from ray.rllib.dqn import DQNAgent, DEFAULT_CONFIG


def main():
    parser = argparse.ArgumentParser(description="Run the DQN algorithm.")
    parser.add_argument("--iterations", default=-1, type=int,
                        help="The number of training iterations to run.")

    args = parser.parse_args()

    config = DEFAULT_CONFIG.copy()
    config.update(dict(
        lr=1e-4,
        schedule_max_timesteps=2000000,
        exploration_fraction=0.1,
        exploration_final_eps=0.01,
        train_freq=4,
        learning_starts=10000,
        target_network_update_freq=1000,
        gamma=0.99,
        prioritized_replay=True))

    ray.init()
    dqn = DQNAgent("PongNoFrameskip-v4", config)

    iteration = 0
    while iteration != args.iterations:
        iteration += 1
        res = dqn.train()
        print("current status: {}".format(res))


if __name__ == "__main__":
    main()

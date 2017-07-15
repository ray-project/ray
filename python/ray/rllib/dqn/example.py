#!/usr/bin/env python

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.rllib.dqn import DQN, DEFAULT_CONFIG


def main():
    config = DEFAULT_CONFIG.copy()
    config.update(dict(
        lr=1e-4,
        schedule_max_timesteps=2000000,
        buffer_size=10000,
        exploration_fraction=0.1,
        exploration_final_eps=0.01,
        train_freq=4,
        learning_starts=10000,
        target_network_update_freq=1000,
        gamma=0.99,
        prioritized_replay=True))

    dqn = DQN("PongNoFrameskip-v4", config)

    while True:
        res = dqn.train()
        print("current status: {}".format(res))


if __name__ == '__main__':
    main()

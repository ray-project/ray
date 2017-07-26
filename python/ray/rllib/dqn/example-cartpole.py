#!/usr/bin/env python

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import tensorflow as tf

import ray
from ray.rllib.dqn import DQN, DEFAULT_CONFIG


def main():
    parser = argparse.ArgumentParser(description="Run the A3C algorithm.")
    parser.add_argument("--iterations", default=-1, type=int,
                        help="The number of training iterations to run.")

    args = parser.parse_args()

    config = DEFAULT_CONFIG.copy()
    config.update(dict(
        lr=1e-3,
        schedule_max_timesteps=100000,
        exploration_fraction=0.1,
        exploration_final_eps=0.02,
        dueling=False,
        hiddens=[],
        model_config=dict(
            fcnet_hiddens=[64],
            fcnet_activation=tf.nn.relu
        )))

    # Currently Ray is not used in this example, but we need to call ray.init
    # to create the directory in which logging will occur. TODO(rkn): Fix this.
    ray.init()

    dqn = DQN("CartPole-v0", config)

    iteration = 0
    while iteration != args.iterations:
        iteration += 1
        res = dqn.train()
        print("current status: {}".format(res))


if __name__ == "__main__":
    main()

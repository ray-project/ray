#!/usr/bin/env python

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse

import ray
from ray.rllib.a3c import A3C, DEFAULT_CONFIG, LSTM, shared_model


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the A3C algorithm.")
    parser.add_argument("--environment", default="PongDeterministic-v3",
                        type=str, help="The gym environment to use.")
    parser.add_argument("--redis-address", default=None, type=str,
                        help="The Redis address of the cluster.")
    parser.add_argument("--num-workers", default=4, type=int,
                        help="The number of A3C workers to use.")
    parser.add_argument("--iterations", default=-1, type=int,
                        help="The number of training iterations to run.")

    args = parser.parse_args()
    ray.init(redis_address=args.redis_address, num_cpus=args.num_workers)

    config = DEFAULT_CONFIG.copy()
    config["num_workers"] = args.num_workers
    # policy_class = LSTM.LSTMPolicy # fix?
    policy_class = shared_model.SharedModel

    a3c = A3C(args.environment, policy_class, config)

    iteration = 0
    while iteration != args.iterations:
        iteration += 1
        res = a3c.train()
        print("current status: {}".format(res))

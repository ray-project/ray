#!/usr/bin/env python

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse

import ray
from ray.rllib.evolution_strategies import EvolutionStrategies, DEFAULT_CONFIG


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Train an RL agent on Pong.")
    parser.add_argument("--num-workers", default=10, type=int,
                        help=("The number of actors to create in aggregate "
                              "across the cluster."))
    parser.add_argument("--env-name", default="Pendulum-v0", type=str,
                        help="The name of the gym environment to use.")
    parser.add_argument("--stepsize", default=0.01, type=float,
                        help="The stepsize to use.")
    parser.add_argument("--redis-address", default=None, type=str,
                        help="The Redis address of the cluster.")
    parser.add_argument("--iterations", default=-1, type=int,
                        help="The number of training iterations to run.")

    args = parser.parse_args()
    num_workers = args.num_workers
    env_name = args.env_name
    stepsize = args.stepsize

    ray.init(redis_address=args.redis_address,
             num_workers=(0 if args.redis_address is None else None))

    config = DEFAULT_CONFIG.copy()
    config["num_workers"] = num_workers
    config["stepsize"] = stepsize

    alg = EvolutionStrategies(env_name, config)
    iteration = 0
    while iteration != args.iterations:
        iteration += 1
        result = alg.train()
        print("current status: {}".format(result))

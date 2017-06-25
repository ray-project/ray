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

  args = parser.parse_args()
  num_workers = args.num_workers
  env_name = args.env_name
  stepsize = args.stepsize

  ray.init(redis_address=args.redis_address,
           num_workers=(0 if args.redis_address is None else None))

  config = DEFAULT_CONFIG._replace(
      num_workers=num_workers,
      stepsize=stepsize)

  alg = EvolutionStrategies(env_name, config)
  while True:
    result = alg.train()
    print("current status: {}".format(result))

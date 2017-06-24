#!/usr/bin/env python

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse

import ray
from ray.rllib.policy_gradient import PolicyGradient, DEFAULT_CONFIG


if __name__ == "__main__":
  parser = argparse.ArgumentParser(description="Run the policy gradient "
                                               "algorithm.")
  parser.add_argument("--environment", default="Pong-v0", type=str,
                      help="The gym environment to use.")
  parser.add_argument("--redis-address", default=None, type=str,
                      help="The Redis address of the cluster.")
  parser.add_argument("--use-tf-debugger", default=False, type=bool,
                      help="Run the script inside of tf-dbg.")
  parser.add_argument("--load-checkpoint", default=None, type=str,
                      help="Continue training from a checkpoint.")

  args = parser.parse_args()
  config = DEFAULT_CONFIG.copy()
  config["use_tf_debugger"] = args.use_tf_debugger
  if args.load_checkpoint:
    config["load_checkpoint"] = args.load_checkpoint

  ray.init(redis_address=args.redis_address)

  alg = PolicyGradient(args.environment, config)
  result = alg.train()
  while result.training_iteration < config["max_iterations"]:
    print("\n== iteration", result.training_iteration)
    result = alg.train()
    print("current status: {}".format(result))

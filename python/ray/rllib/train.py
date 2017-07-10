#!/usr/bin/env python

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import json
import os

import ray
import ray.rllib.policy_gradient as pg
import ray.rllib.evolution_strategies as es
import ray.rllib.dqn as dqn
import ray.rllib.a3c as a3c

parser = argparse.ArgumentParser(
    description=("Train a reinforcement learning agent."))
parser.add_argument("--env", required=True, type=str)
parser.add_argument("--alg", required=True, type=str)
parser.add_argument("--upload-dir", default="file:///tmp/ray", type=str)


if __name__ == "__main__":
  args = parser.parse_args()

  ray.init()

  env_name = args.env
  if args.alg == "PolicyGradient":
    alg = pg.PolicyGradient(
        env_name, pg.DEFAULT_CONFIG, upload_dir=args.upload_dir)
  elif args.alg == "EvolutionStrategies":
    alg = es.EvolutionStrategies(
        env_name, es.DEFAULT_CONFIG, upload_dir=args.upload_dir)
  elif args.alg == "DQN":
    alg = dqn.DQN(
        env_name, dqn.DEFAULT_CONFIG, upload_dir=args.upload_dir)
  elif args.alg == "A3C":
    alg = a3c.A3C(
        env_name, a3c.DEFAULT_CONFIG, upload_dir=args.upload_dir)
  else:
    assert False, ("Unknown algorithm, check --alg argument. Valid choices "
                   "are PolicyGradientPolicyGradient, EvolutionStrategies, "
                   "DQN and A3C.")

  result_logger = ray.rllib.common.RLLibLogger(
      os.path.join(alg.logdir, "result.json"))

  while True:
    result = alg.train()

    # We need to use a custom json serializer class so that NaNs get encoded
    # as null as required by Athena.
    json.dump(result._asdict(), result_logger,
              cls=ray.rllib.common.RLLibEncoder)
    result_logger.write("\n")

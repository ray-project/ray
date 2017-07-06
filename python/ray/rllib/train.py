from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import json
import ray
import ray.rllib.policy_gradient as pg
import ray.rllib.evolution_strategies as es
# import ray.rllib.a3c as a3c

parser = argparse.ArgumentParser(
    description=("Train a reinforcement learning agent."))
parser.add_argument("--env", required=True, type=str)
parser.add_argument("--alg", required=True, type=str)
parser.add_argument("--s3-bucket", required=False, type=str)


if __name__ == "__main__":
  args = parser.parse_args()

  ray.init()

  env_name = args.env
  # TODO(pcm): Implement logging for EvolutionStrategies and A3C
  if args.alg == "PolicyGradient":
    alg = pg.PolicyGradient(env_name, pg.DEFAULT_CONFIG, args.s3_bucket)
  elif args.alg == "EvolutionStrategies":
    alg = es.EvolutionStrategies(env_name, es.DEFAULT_CONFIG, args.s3_bucket)
  elif args.alg == "A3C":
    alg = a3c.A3C(env_name, a3c.DEFAULT_CONFIG, args.s3_bucket)
  else:
    assert False, "Unknown algorithm, check --alg argument. Valid choices are" \
                  "PolicyGradientPolicyGradient, EvolutionStrategies and A3C."
  if args.s3_bucket:
    result_logger = ray.rllib.common.S3Logger(args.s3_bucket + "/" + alg.logprefix + "/" + "result.json")
    info_logger = ray.rllib.common.S3Logger(args.s3_bucket + "/" + alg.logprefix + "/" + "info.json")

  while True:
    result, info = alg.train()
    if args.s3_bucket:
      json.dump(result._asdict(), result_logger)
      result_logger.write("\n")
      json.dump(info._asdict(), info_logger)
      info_logger.write("\n")

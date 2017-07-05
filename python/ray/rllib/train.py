from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import json
import ray
import ray.rllib.policy_gradient as pg

parser = argparse.ArgumentParser(
    description=("Train a reinforcement learning agent."))
parser.add_argument("--s3-bucket", required=False, type=str)


if __name__ == "__main__":
  args = parser.parse_args()

  ray.init()

  env_name = "CartPole-v0"
  alg = pg.PolicyGradient(env_name, pg.DEFAULT_CONFIG, args.s3_bucket)
  if args.s3_bucket:
    logger = ray.rllib.common.S3Logger(args.s3_bucket + "/" + alg.logprefix + "/" + "result.json")

  while True:
    result, info = alg.train()
    print("policy gradient: {}".format(result))
    if args.s3_bucket:
      json.dump(result._asdict(), logger)
      logger.write("\n")

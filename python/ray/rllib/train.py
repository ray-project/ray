from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import json
try:
    import smart_open
except ImportError:
    pass
import ray
import ray.rllib.policy_gradient as pg

parser = argparse.ArgumentParser(
    description=("Train a reinforcement learning agent."))
parser.add_argument("--s3-bucket", required=False, type=str)


if __name__ == "__main__":
  args = parser.parse_args()

  ray.init()

  # TODO(ekl): get the algorithms working on a common set of envs
  env_name = "CartPole-v0"
  alg = pg.PolicyGradient(env_name, pg.DEFAULT_CONFIG, args.s3_bucket)

  with smart_open.smart_open(args.s3_bucket + "/" + alg.logprefix + "/" + "result.json", "wb") as f:
    while True:
      r = alg.train()
      print("policy gradient: {}".format(r))
      json.dump(r, f, sort_keys=True, indent=4)

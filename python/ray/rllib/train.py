from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray
import ray.rllib.policy_gradient as pg

if __name__ == "__main__":
  ray.init()

  # TODO(ekl): get the algorithms working on a common set of envs
  env_name = "CartPole-v0"
  alg = pg.PolicyGradient(env_name, pg.DEFAULT_CONFIG)

  while True:
    r = alg.train()
    print("policy gradient: {}".format(r))

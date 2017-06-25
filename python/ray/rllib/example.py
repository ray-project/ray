#!/usr/bin/env python
"""Demonstrates the RLlib algorithm API through a simple bakeoff."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray
import ray.rllib.evolution_strategies as es
import ray.rllib.policy_gradient as pg


if __name__ == "__main__":
  ray.init()

  # TODO(ekl): get the algorithms working on a common set of envs
  env_name = "CartPole-v0"
  alg1 = es.EvolutionStrategies(env_name, es.DEFAULT_CONFIG)
  alg2 = pg.PolicyGradient(env_name, pg.DEFAULT_CONFIG)

  while True:
    r1 = alg1.train()
    r2 = alg2.train()
    print("evolution strategies: {}".format(r1))
    print("policy gradient: {}".format(r2))

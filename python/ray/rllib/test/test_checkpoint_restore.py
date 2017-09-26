#!/usr/bin/env python

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray
import random

from ray.rllib.dqn import (DQNAgent, DEFAULT_CONFIG as DQN_CONFIG)
from ray.rllib.es import (ESAgent, DEFAULT_CONFIG as ES_CONFIG)
from ray.rllib.ppo import (PPOAgent, DEFAULT_CONFIG as PG_CONFIG)
from ray.rllib.a3c import (A3CAgent, DEFAULT_CONFIG as A3C_CONFIG)

ray.init()
for (cls, default_config) in [
#        (DQNAgent, DQN_CONFIG),
        (PPOAgent, PG_CONFIG),
        # TODO(ekl) this fails with multiple ES instances in a process
#        (ESAgent, ES_CONFIG),
#        (A3CAgent, A3C_CONFIG)
    ]:
    config = default_config.copy()
    config["num_sgd_iter"] = 5
    config["episodes_per_batch"] = 100
    config["timesteps_per_batch"] = 1000
    alg1 = cls('CartPole-v0', config)
    alg2 = cls('CartPole-v0', config)

    for _ in range(3):
        res = alg1.train()
        print("current status: " + str(res))

    # Sync the models
    alg2.restore(alg1.save())

    for _ in range(10):
        obs = [
            random.random(), random.random(), random.random(), random.random()]
        a1 = alg1.compute_action(obs)
        a2 = alg2.compute_action(obs)
        print("Checking computed actions", obs, a1, a2)

        # TODO(ekl) this fails for stochastic policies
        assert(a1 == a2)

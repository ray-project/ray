#!/usr/bin/env python

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import ray
import random

from ray.rllib.dqn import (DQNAgent, DEFAULT_CONFIG as DQN_CONFIG)
from ray.rllib.ppo import (PPOAgent, DEFAULT_CONFIG as PG_CONFIG)
from ray.rllib.a3c import (A3CAgent, DEFAULT_CONFIG as A3C_CONFIG)
# from ray.rllib.es import (ESAgent, DEFAULT_CONFIG as ES_CONFIG)


def get_mean_action(alg, obs):
    out = []
    for _ in range(2000):
        out.append(float(alg.compute_action(obs)))
    return np.mean(out)


ray.init()
for (cls, default_config) in [
        (DQNAgent, DQN_CONFIG),
        (PPOAgent, PG_CONFIG),
        (A3CAgent, A3C_CONFIG),
        # https://github.com/ray-project/ray/issues/1062
        # (ESAgent, ES_CONFIG),
        ]:
    config = default_config.copy()
    config["num_sgd_iter"] = 5
    config["use_lstm"] = False  # for a3c
    config["episodes_per_batch"] = 100
    config["timesteps_per_batch"] = 1000
    alg1 = cls("CartPole-v0", config)
    alg2 = cls("CartPole-v0", config)

    for _ in range(3):
        res = alg1.train()
        print("current status: " + str(res))

    # Sync the models
    alg2.restore(alg1.save())

    for _ in range(10):
        obs = [
            random.random(), random.random(), random.random(), random.random()]
        a1 = get_mean_action(alg1, obs)
        a2 = get_mean_action(alg2, obs)
        print("Checking computed actions", alg1, obs, a1, a2)
        assert abs(a1-a2) < .1, (a1, a2)

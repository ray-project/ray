#!/usr/bin/env python

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import ray

from ray.rllib.agent import get_agent_class


def get_mean_action(alg, obs):
    out = []
    for _ in range(2000):
        out.append(float(alg.compute_action(obs)))
    return np.mean(out)


ray.init()

CONFIGS = {
    "ES": {"episodes_per_batch": 10, "timesteps_per_batch": 100},
    "DQN": {},
    "PPO": {"num_sgd_iter": 5, "timesteps_per_batch": 1000},
    "A3C": {"use_lstm": False},
}

for name in ["ES", "DQN", "PPO", "A3C"]:
    cls = get_agent_class(name)
    alg1 = cls("CartPole-v0", CONFIGS[name])
    alg2 = cls("CartPole-v0", CONFIGS[name])

    for _ in range(3):
        res = alg1.train()
        print("current status: " + str(res))

    # Sync the models
    alg2.restore(alg1.save())

    for _ in range(10):
        obs = np.random.uniform(size=4)
        a1 = get_mean_action(alg1, obs)
        a2 = get_mean_action(alg2, obs)
        print("Checking computed actions", alg1, obs, a1, a2)
        assert abs(a1-a2) < .1, (a1, a2)

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


def test(use_object_store, alg_name):
    cls = get_agent_class(alg_name)
    alg1 = cls(config=CONFIGS[name], env="CartPole-v0")
    alg2 = cls(config=CONFIGS[name], env="CartPole-v0")

    for _ in range(3):
        res = alg1.train()
        print("current status: " + str(res))

    # Sync the models
    if use_object_store:
        alg2.restore_from_object(alg1.save_to_object())
    else:
        alg2.restore(alg1.save())

    for _ in range(10):
        obs = np.random.uniform(size=4)
        a1 = get_mean_action(alg1, obs)
        a2 = get_mean_action(alg2, obs)
        print("Checking computed actions", alg1, obs, a1, a2)
        assert abs(a1 - a2) < .1, (a1, a2)


if __name__ == "__main__":
    # https://github.com/ray-project/ray/issues/1062 for enabling ES test too
    for use_object_store in [False, True]:
        for name in ["ES", "DQN", "PPO", "A3C"]:
            test(use_object_store, name)

    print("All checkpoint restore tests passed!")

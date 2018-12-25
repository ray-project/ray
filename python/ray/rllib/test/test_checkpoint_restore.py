#!/usr/bin/env python

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import shutil
import numpy as np
import ray

from ray.rllib.agents.registry import get_agent_class


def get_mean_action(alg, obs):
    out = []
    for _ in range(2000):
        out.append(float(alg.compute_action(obs)))
    return np.mean(out)


ray.init(num_cpus=10)

CONFIGS = {
    "ES": {
        "episodes_per_batch": 10,
        "train_batch_size": 100,
        "num_workers": 2,
        "observation_filter": "MeanStdFilter"
    },
    "DQN": {},
    "APEX_DDPG": {
        "observation_filter": "MeanStdFilter",
        "num_workers": 2,
        "min_iter_time_s": 1,
        "optimizer": {
            "num_replay_buffer_shards": 1,
        },
    },
    "DDPG": {
        "noise_scale": 0.0,
        "timesteps_per_iteration": 100
    },
    "PPO": {
        "num_sgd_iter": 5,
        "train_batch_size": 1000,
        "num_workers": 2
    },
    "A3C": {
        "num_workers": 1
    },
    "ARS": {
        "num_rollouts": 10,
        "num_workers": 2,
        "observation_filter": "MeanStdFilter"
    }
}


def test_ckpt_restore(use_object_store, alg_name, failures):
    cls = get_agent_class(alg_name)
    if "DDPG" in alg_name:
        alg1 = cls(config=CONFIGS[name], env="Pendulum-v0")
        alg2 = cls(config=CONFIGS[name], env="Pendulum-v0")
    else:
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
        if "DDPG" in alg_name:
            obs = np.random.uniform(size=3)
        else:
            obs = np.random.uniform(size=4)
        a1 = get_mean_action(alg1, obs)
        a2 = get_mean_action(alg2, obs)
        print("Checking computed actions", alg1, obs, a1, a2)
        if abs(a1 - a2) > .1:
            failures.append((alg_name, [a1, a2]))


def test_export(algo_name, failures):
    cls = get_agent_class(algo_name)
    if "DDPG" in algo_name:
        algo = cls(config=CONFIGS[name], env="Pendulum-v0")
    else:
        algo = cls(config=CONFIGS[name], env="CartPole-v0")

    for _ in range(3):
        res = algo.train()
        print("current status: " + str(res))

    export_dir = "/tmp/export_dir_%s" % algo_name
    print("Exporting model ", algo_name, export_dir)
    algo.export_policy_model(export_dir)
    if not os.path.exists(os.path.join(export_dir, "saved_model.pb")) \
            or not os.listdir(os.path.join(export_dir, "variables")):
        failures.append(algo_name)
    shutil.rmtree(export_dir)


if __name__ == "__main__":
    failures = []
    for use_object_store in [False, True]:
        for name in ["ES", "DQN", "DDPG", "PPO", "A3C", "APEX_DDPG", "ARS"]:
            test_ckpt_restore(use_object_store, name, failures)

    assert not failures, failures
    print("All checkpoint restore tests passed!")

    failures = []
    for name in ["DQN", "DDPG", "PPO", "A3C"]:
        test_export(name, failures)
    assert not failures, failures
    print("All export tests passed!")

#!/usr/bin/env python

import numpy as np
import unittest

import ray
from ray.rllib.agents.registry import get_agent_class
from ray.rllib.utils.test_utils import framework_iterator


def get_mean_action(alg, obs):
    out = []
    for _ in range(2000):
        out.append(float(alg.compute_action(obs)))
    return np.mean(out)


CONFIGS = {
    "A3C": {
        "explore": False,
        "num_workers": 1,
    },
    "APEX_DDPG": {
        "explore": False,
        "observation_filter": "MeanStdFilter",
        "num_workers": 2,
        "min_iter_time_s": 1,
        "optimizer": {
            "num_replay_buffer_shards": 1,
        },
    },
    "ARS": {
        "explore": False,
        "num_rollouts": 10,
        "num_workers": 2,
        "noise_size": 2500000,
        "observation_filter": "MeanStdFilter",
    },
    "DDPG": {
        "explore": False,
        "timesteps_per_iteration": 100,
    },
    "DQN": {
        "explore": False,
    },
    "ES": {
        "explore": False,
        "episodes_per_batch": 10,
        "train_batch_size": 100,
        "num_workers": 2,
        "noise_size": 2500000,
        "observation_filter": "MeanStdFilter",
    },
    "PPO": {
        "explore": False,
        "num_sgd_iter": 5,
        "train_batch_size": 1000,
        "num_workers": 2,
    },
    "SAC": {
        "explore": False,
    },
}


def ckpt_restore_test(use_object_store, alg_name, failures, framework="tf"):
    cls = get_agent_class(alg_name)
    config = CONFIGS[alg_name]
    config["framework"] = framework
    if "DDPG" in alg_name or "SAC" in alg_name:
        alg1 = cls(config=config, env="Pendulum-v0")
        alg2 = cls(config=config, env="Pendulum-v0")
    else:
        alg1 = cls(config=config, env="CartPole-v0")
        alg2 = cls(config=config, env="CartPole-v0")

    policy1 = alg1.get_policy()

    for _ in range(1):
        res = alg1.train()
        print("current status: " + str(res))

    # Sync the models
    if use_object_store:
        alg2.restore_from_object(alg1.save_to_object())
    else:
        alg2.restore(alg1.save())

    for _ in range(1):
        if "DDPG" in alg_name or "SAC" in alg_name:
            obs = np.clip(
                np.random.uniform(size=3),
                policy1.observation_space.low,
                policy1.observation_space.high)
        else:
            obs = np.clip(
                np.random.uniform(size=4),
                policy1.observation_space.low,
                policy1.observation_space.high)
        a1 = get_mean_action(alg1, obs)
        a2 = get_mean_action(alg2, obs)
        print("Checking computed actions", alg1, obs, a1, a2)
        if abs(a1 - a2) > .1:
            failures.append((alg_name, [a1, a2]))


class TestCheckpointRestore(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init(num_cpus=10, object_store_memory=1e9)

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_checkpoint_restore(self):
        failures = []
        for fw in framework_iterator(frameworks=("tf", "torch")):
            for use_object_store in [False, True]:
                for name in [
                        "A3C", "APEX_DDPG", "ARS", "DDPG", "DQN", "ES", "PPO",
                        "SAC"
                ]:
                    print("Testing algo={} (use_object_store={})".format(
                        name, use_object_store))
                    ckpt_restore_test(
                        use_object_store, name, failures, framework=fw)

        assert not failures, failures
        print("All checkpoint restore tests passed!")


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))

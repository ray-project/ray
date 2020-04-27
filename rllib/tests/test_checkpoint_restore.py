#!/usr/bin/env python

import gym
import numpy as np
import os
import shutil
import unittest

import ray
from ray.rllib.agents.registry import get_agent_class
from ray.tune.trial import ExportFormat


def get_mean_action(alg, obs):
    out = []
    for _ in range(2000):
        out.append(float(alg.compute_action(obs)))
    return np.mean(out)


CONFIGS = {
    "A3C": {
        "explore": False,
        "num_workers": 1
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
        "observation_filter": "MeanStdFilter"
    },
    "DDPG": {
        "explore": False,
        "timesteps_per_iteration": 100
    },
    "DQN": {
        "explore": False
    },
    "ES": {
        "explore": False,
        "episodes_per_batch": 10,
        "train_batch_size": 100,
        "num_workers": 2,
        "noise_size": 2500000,
        "observation_filter": "MeanStdFilter"
    },
    "PPO": {
        "explore": False,
        "num_sgd_iter": 5,
        "train_batch_size": 1000,
        "num_workers": 2
    },
    "SAC": {
        "explore": False,
    },
}


def ckpt_restore_test(use_object_store, alg_name, failures):
    cls = get_agent_class(alg_name)
    if "DDPG" in alg_name or "SAC" in alg_name:
        alg1 = cls(config=CONFIGS[alg_name], env="Pendulum-v0")
        alg2 = cls(config=CONFIGS[alg_name], env="Pendulum-v0")
        env = gym.make("Pendulum-v0")
    else:
        alg1 = cls(config=CONFIGS[alg_name], env="CartPole-v0")
        alg2 = cls(config=CONFIGS[alg_name], env="CartPole-v0")
        env = gym.make("CartPole-v0")

    for _ in range(1):
        res = alg1.train()
        print("current status: " + str(res))

    # Sync the models
    if use_object_store:
        alg2.restore_from_object(alg1.save_to_object())
    else:
        alg2.restore(alg1.save())

    for _ in range(2):
        if "DDPG" in alg_name or "SAC" in alg_name:
            obs = np.clip(
                np.random.uniform(size=3),
                env.observation_space.low,
                env.observation_space.high)
        else:
            obs = np.clip(
                np.random.uniform(size=4),
                env.observation_space.low,
                env.observation_space.high)
        a1 = get_mean_action(alg1, obs)
        a2 = get_mean_action(alg2, obs)
        print("Checking computed actions", alg1, obs, a1, a2)
        if abs(a1 - a2) > .1:
            failures.append((alg_name, [a1, a2]))


def export_test(alg_name, failures):
    def valid_tf_model(model_dir):
        return os.path.exists(os.path.join(model_dir, "saved_model.pb")) \
            and os.listdir(os.path.join(model_dir, "variables"))

    def valid_tf_checkpoint(checkpoint_dir):
        return os.path.exists(os.path.join(checkpoint_dir, "model.meta")) \
            and os.path.exists(os.path.join(checkpoint_dir, "model.index")) \
            and os.path.exists(os.path.join(checkpoint_dir, "checkpoint"))

    cls = get_agent_class(alg_name)
    if "DDPG" in alg_name or "SAC" in alg_name:
        algo = cls(config=CONFIGS[alg_name], env="Pendulum-v0")
    else:
        algo = cls(config=CONFIGS[alg_name], env="CartPole-v0")

    for _ in range(1):
        res = algo.train()
        print("current status: " + str(res))

    export_dir = os.path.join(ray.utils.get_user_temp_dir(),
                              "export_dir_%s" % alg_name)
    print("Exporting model ", alg_name, export_dir)
    algo.export_policy_model(export_dir)
    if not valid_tf_model(export_dir):
        failures.append(alg_name)
    shutil.rmtree(export_dir)

    print("Exporting checkpoint", alg_name, export_dir)
    algo.export_policy_checkpoint(export_dir)
    if not valid_tf_checkpoint(export_dir):
        failures.append(alg_name)
    shutil.rmtree(export_dir)

    print("Exporting default policy", alg_name, export_dir)
    algo.export_model([ExportFormat.CHECKPOINT, ExportFormat.MODEL],
                      export_dir)
    if not valid_tf_model(os.path.join(export_dir, ExportFormat.MODEL)) \
            or not valid_tf_checkpoint(os.path.join(export_dir,
                                                    ExportFormat.CHECKPOINT)):
        failures.append(alg_name)
    shutil.rmtree(export_dir)


class TestCheckpointRestore(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init(num_cpus=10, object_store_memory=1e9)

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_checkpoint_restore(self):
        failures = []
        for use_object_store in [False, True]:
            for name in [
                    "SAC", "ES", "DQN", "DDPG", "PPO", "A3C", "APEX_DDPG",
                    "ARS"
            ]:
                ckpt_restore_test(use_object_store, name, failures)

        assert not failures, failures
        print("All checkpoint restore tests passed!")

        failures = []
        for name in ["SAC", "DQN", "DDPG", "PPO", "A3C"]:
            export_test(name, failures)
        assert not failures, failures
        print("All export tests passed!")


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))

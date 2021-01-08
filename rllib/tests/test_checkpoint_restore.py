#!/usr/bin/env python

import numpy as np
import unittest

import ray
from ray.rllib.agents.registry import get_agent_class
from ray.rllib.utils.test_utils import check, framework_iterator


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


def ckpt_restore_test(alg_name, tfe=False):
    config = CONFIGS[alg_name]
    frameworks = (["tfe"] if tfe else []) + ["torch", "tf"]
    for fw in framework_iterator(config, frameworks=frameworks):
        for use_object_store in [False, True]:
            print("use_object_store={}".format(use_object_store))
            cls = get_agent_class(alg_name)
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

            # Check optimizer state as well.
            optim_state = policy1.get_state().get("_optimizer_variables")

            # Sync the models
            if use_object_store:
                alg2.restore_from_object(alg1.save_to_object())
            else:
                alg2.restore(alg1.save())

            # Compare optimizer state with re-loaded one.
            if optim_state:
                s2 = alg2.get_policy().get_state().get("_optimizer_variables")
                # Tf -> Compare states 1:1.
                if fw in ["tf2", "tf", "tfe"]:
                    check(s2, optim_state)
                # For torch, optimizers have state_dicts with keys=params,
                # which are different for the two models (ignore these
                # different keys, but compare all values nevertheless).
                else:
                    for i, s2_ in enumerate(s2):
                        check(
                            list(s2_["state"].values()),
                            list(optim_state[i]["state"].values()))

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
                    raise AssertionError("algo={} [a1={} a2={}]".format(
                        alg_name, a1, a2))
            # Stop both Trainers.
            alg1.stop()
            alg2.stop()


class TestCheckpointRestore(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init(num_cpus=10, object_store_memory=1e9)

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_a3c_checkpoint_restore(self):
        ckpt_restore_test("A3C")

    def test_apex_ddpg_checkpoint_restore(self):
        ckpt_restore_test("APEX_DDPG")

    def test_ars_checkpoint_restore(self):
        ckpt_restore_test("ARS")

    def test_ddpg_checkpoint_restore(self):
        ckpt_restore_test("DDPG")

    def test_dqn_checkpoint_restore(self):
        ckpt_restore_test("DQN")

    def test_es_checkpoint_restore(self):
        ckpt_restore_test("ES")

    def test_ppo_checkpoint_restore(self):
        ckpt_restore_test("PPO")

    def test_sac_checkpoint_restore(self):
        ckpt_restore_test("SAC")


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))

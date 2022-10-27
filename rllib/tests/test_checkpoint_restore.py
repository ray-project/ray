#!/usr/bin/env python

import numpy as np
import unittest

import ray
from ray.rllib.algorithms.registry import get_algorithm_class
from ray.rllib.utils.test_utils import check, framework_iterator


def get_mean_action(alg, obs):
    out = []
    for _ in range(2000):
        out.append(float(alg.compute_single_action(obs)))
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
        "min_time_s_per_iteration": 1,
        "optimizer": {
            "num_replay_buffer_shards": 1,
        },
        "num_steps_sampled_before_learning_starts": 0,
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
        "min_sample_timesteps_per_iteration": 100,
        "num_steps_sampled_before_learning_starts": 0,
    },
    "DQN": {
        "explore": False,
        "num_steps_sampled_before_learning_starts": 0,
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
    "SimpleQ": {
        "explore": False,
        "num_steps_sampled_before_learning_starts": 0,
    },
    "SAC": {
        "explore": False,
        "num_steps_sampled_before_learning_starts": 0,
    },
}


def ckpt_restore_test(alg_name, tf2=False, object_store=False, replay_buffer=False):
    config = CONFIGS[alg_name].copy()
    # If required, store replay buffer data in checkpoints as well.
    if replay_buffer:
        config["store_buffer_in_checkpoints"] = True

    frameworks = (["tf2"] if tf2 else []) + ["torch", "tf"]
    for fw in framework_iterator(config, frameworks=frameworks):
        for use_object_store in [False, True] if object_store else [False]:
            print("use_object_store={}".format(use_object_store))
            cls = get_algorithm_class(alg_name)
            if "DDPG" in alg_name or "SAC" in alg_name:
                alg1 = cls(config=config, env="Pendulum-v1")
                alg2 = cls(config=config, env="Pendulum-v1")
            else:
                alg1 = cls(config=config, env="CartPole-v1")
                alg2 = cls(config=config, env="CartPole-v1")

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
                if fw in ["tf2", "tf"]:
                    check(s2, optim_state)
                # For torch, optimizers have state_dicts with keys=params,
                # which are different for the two models (ignore these
                # different keys, but compare all values nevertheless).
                else:
                    for i, s2_ in enumerate(s2):
                        check(
                            list(s2_["state"].values()),
                            list(optim_state[i]["state"].values()),
                        )

            # Compare buffer content with restored one.
            if replay_buffer:
                data = alg1.local_replay_buffer.replay_buffers[
                    "default_policy"
                ]._storage[42 : 42 + 42]
                new_data = alg2.local_replay_buffer.replay_buffers[
                    "default_policy"
                ]._storage[42 : 42 + 42]
                check(data, new_data)

            for _ in range(1):
                if "DDPG" in alg_name or "SAC" in alg_name:
                    obs = np.clip(
                        np.random.uniform(size=3),
                        policy1.observation_space.low,
                        policy1.observation_space.high,
                    )
                else:
                    obs = np.clip(
                        np.random.uniform(size=4),
                        policy1.observation_space.low,
                        policy1.observation_space.high,
                    )
                a1 = get_mean_action(alg1, obs)
                a2 = get_mean_action(alg2, obs)
                print("Checking computed actions", alg1, obs, a1, a2)
                if abs(a1 - a2) > 0.1:
                    raise AssertionError(
                        "algo={} [a1={} a2={}]".format(alg_name, a1, a2)
                    )
            # Stop both algos.
            alg1.stop()
            alg2.stop()


class TestCheckpointRestorePG(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init(num_cpus=5)

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_a3c_checkpoint_restore(self):
        ckpt_restore_test("A3C")

    def test_ppo_checkpoint_restore(self):
        ckpt_restore_test("PPO", object_store=True)


class TestCheckpointRestoreOffPolicy(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init(num_cpus=5)

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_apex_ddpg_checkpoint_restore(self):
        ckpt_restore_test("APEX_DDPG")

    def test_ddpg_checkpoint_restore(self):
        ckpt_restore_test("DDPG", replay_buffer=True)

    def test_dqn_checkpoint_restore(self):
        ckpt_restore_test("DQN", object_store=True, replay_buffer=True)

    def test_sac_checkpoint_restore(self):
        ckpt_restore_test("SAC", replay_buffer=True)

    def test_simpleq_checkpoint_restore(self):
        ckpt_restore_test("SimpleQ", replay_buffer=True)


class TestCheckpointRestoreEvolutionAlgos(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init(num_cpus=5)

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_ars_checkpoint_restore(self):
        ckpt_restore_test("ARS")

    def test_es_checkpoint_restore(self):
        ckpt_restore_test("ES")


if __name__ == "__main__":
    import pytest
    import sys

    # One can specify the specific TestCase class to run.
    # None for all unittest.TestCase classes in this file.
    class_ = sys.argv[1] if len(sys.argv) > 1 else None
    sys.exit(pytest.main(["-v", __file__ + ("" if class_ is None else "::" + class_)]))

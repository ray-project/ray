import gym
import unittest

import ray
from ray import air
from ray import tune
from ray.rllib.algorithms.a2c import A2CConfig
from ray.rllib.examples.env.stateless_cartpole import StatelessCartPole
from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.examples.models.neural_computer import DNCMemory
from ray.rllib.utils.framework import try_import_torch

torch, _ = try_import_torch()


class TestDNC(unittest.TestCase):
    stop = {
        "episode_reward_mean": 100.0,
        "timesteps_total": 10000000,
    }

    @classmethod
    def setUpClass(cls) -> None:
        ray.init(num_cpus=4, ignore_reinit_error=True)

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_pack_unpack(self):
        d = DNCMemory(gym.spaces.Discrete(1), gym.spaces.Discrete(1), 1, {}, "")
        # Add batch dim
        packed_state = [m.unsqueeze(0) for m in d.get_initial_state()]
        [m.random_() for m in packed_state]
        original_packed = [m.clone() for m in packed_state]

        B, T = packed_state[0].shape[:2]
        unpacked = d.unpack_state(packed_state)
        packed = d.pack_state(*unpacked)

        self.assertTrue(len(packed) > 0)
        self.assertEqual(len(packed), len(original_packed))

        for m_idx in range(len(packed)):
            self.assertTrue(torch.all(packed[m_idx] == original_packed[m_idx]))

    def test_dnc_learning(self):
        ModelCatalog.register_custom_model("dnc", DNCMemory)

        config = (
            A2CConfig()
            .environment(StatelessCartPole)
            .framework("torch")
            .rollouts(num_envs_per_worker=5, num_rollout_workers=1)
            .training(
                gamma=0.99,
                lr=0.01,
                entropy_coeff=0.0005,
                vf_loss_coeff=1e-5,
                model={
                    "custom_model": "dnc",
                    "max_seq_len": 64,
                    "custom_model_config": {
                        "nr_cells": 10,
                        "cell_size": 8,
                    },
                },
            )
            .resources(num_cpus_per_worker=2.0)
        )

        tune.Tuner(
            "A2C",
            param_space=config,
            run_config=air.RunConfig(stop=self.stop, verbose=1),
        ).fit()


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))

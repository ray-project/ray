#!/usr/bin/env python
import unittest

import ray

from ray.rllib.algorithms.apex_ddpg import ApexDDPGConfig
from ray.rllib.algorithms.sac import SACConfig
from ray.rllib.algorithms.simple_q import SimpleQConfig
from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.algorithms.es import ESConfig
from ray.rllib.algorithms.dqn import DQNConfig
from ray.rllib.algorithms.ddpg import DDPGConfig
from ray.rllib.algorithms.ars import ARSConfig
from ray.rllib.algorithms.a3c import A3CConfig
from ray.rllib.utils.test_utils import test_ckpt_restore
import os


# As we transition things to RLModule API the explore=False will get
# deprecated. For now, we will just not set it. The reason is that the RLModule
# API has forward_exploration() method that can be overriden if user needs to
# really turn of the stochasticity. This test in particular is robust to
# explore=None if we compare the mean of the distribution of actions for the
# same observation to be the same.
algorithms_and_configs = {
    "A3C": (
        A3CConfig()
        .exploration(explore=False)
        .rollouts(num_rollout_workers=1)
        .resources(num_gpus=int(os.environ.get("RLLIB_NUM_GPUS", "0")))
    ),
    "APEX_DDPG": (
        ApexDDPGConfig()
        .exploration(explore=False)
        .rollouts(observation_filter="MeanStdFilter", num_rollout_workers=2)
        .reporting(min_time_s_per_iteration=1)
        .training(
            optimizer={"num_replay_buffer_shards": 1},
            num_steps_sampled_before_learning_starts=0,
        )
        .resources(num_gpus=int(os.environ.get("RLLIB_NUM_GPUS", "0")))
    ),
    "ARS": (
        ARSConfig()
        .exploration(explore=False)
        .rollouts(num_rollout_workers=2, observation_filter="MeanStdFilter")
        .training(num_rollouts=10, noise_size=2500000)
        .resources(num_gpus=int(os.environ.get("RLLIB_NUM_GPUS", "0")))
    ),
    "DDPG": (
        DDPGConfig()
        .exploration(explore=False)
        .reporting(min_sample_timesteps_per_iteration=100)
        .training(num_steps_sampled_before_learning_starts=0)
        .resources(num_gpus=int(os.environ.get("RLLIB_NUM_GPUS", "0")))
    ),
    "DQN": (
        DQNConfig()
        .exploration(explore=False)
        .training(num_steps_sampled_before_learning_starts=0)
        .resources(num_gpus=int(os.environ.get("RLLIB_NUM_GPUS", "0")))
    ),
    "ES": (
        ESConfig()
        .exploration(explore=False)
        .training(episodes_per_batch=10, train_batch_size=100, noise_size=2500000)
        .rollouts(observation_filter="MeanStdFilter", num_rollout_workers=2)
        .resources(num_gpus=int(os.environ.get("RLLIB_NUM_GPUS", "0")))
    ),
    "PPO": (
        # See the comment before the `algorithms_and_configs` dict.
        # explore is set to None for PPO in favor of RLModule API support.
        PPOConfig()
        .training(num_sgd_iter=5, train_batch_size=1000)
        .rollouts(num_rollout_workers=2)
        .resources(num_gpus=int(os.environ.get("RLLIB_NUM_GPUS", "0")))
    ),
    "SimpleQ": (
        SimpleQConfig()
        .exploration(explore=False)
        .training(num_steps_sampled_before_learning_starts=0)
        .resources(num_gpus=int(os.environ.get("RLLIB_NUM_GPUS", "0")))
    ),
    "SAC": (
        SACConfig()
        .exploration(explore=False)
        .training(num_steps_sampled_before_learning_starts=0)
        .resources(num_gpus=int(os.environ.get("RLLIB_NUM_GPUS", "0")))
    ),
}


class TestCheckpointRestorePG(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init()

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_a3c_checkpoint_restore(self):
        # TODO(Kourosh) A3C cannot run a restored algorithm for some reason.
        test_ckpt_restore(
            algorithms_and_configs["A3C"], "CartPole-v1", run_restored_algorithm=False
        )

    def test_ppo_checkpoint_restore(self):
        test_ckpt_restore(
            algorithms_and_configs["PPO"], "CartPole-v1", object_store=True
        )


class TestCheckpointRestoreOffPolicy(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init()

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_apex_ddpg_checkpoint_restore(self):
        test_ckpt_restore(algorithms_and_configs["APEX_DDPG"], "Pendulum-v1")

    def test_ddpg_checkpoint_restore(self):
        test_ckpt_restore(
            algorithms_and_configs["DDPG"], "Pendulum-v1", replay_buffer=True
        )

    def test_dqn_checkpoint_restore(self):
        test_ckpt_restore(
            algorithms_and_configs["DQN"],
            "CartPole-v1",
            object_store=True,
            replay_buffer=True,
        )

    def test_sac_checkpoint_restore(self):
        test_ckpt_restore(
            algorithms_and_configs["SAC"], "Pendulum-v1", replay_buffer=True
        )

    def test_simpleq_checkpoint_restore(self):
        test_ckpt_restore(
            algorithms_and_configs["SimpleQ"], "CartPole-v1", replay_buffer=True
        )


class TestCheckpointRestoreEvolutionAlgos(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init()

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_ars_checkpoint_restore(self):
        test_ckpt_restore(algorithms_and_configs["ARS"], "CartPole-v1")

    def test_es_checkpoint_restore(self):
        test_ckpt_restore(algorithms_and_configs["ES"], "CartPole-v1")


if __name__ == "__main__":
    import pytest
    import sys

    # One can specify the specific TestCase class to run.
    # None for all unittest.TestCase classes in this file.
    class_ = sys.argv[1] if len(sys.argv) > 1 else None
    sys.exit(pytest.main(["-v", __file__ + ("" if class_ is None else "::" + class_)]))

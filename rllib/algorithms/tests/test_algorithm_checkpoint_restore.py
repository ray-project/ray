#!/usr/bin/env python
import unittest

import ray

from ray.rllib.algorithms.sac import SACConfig
from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.algorithms.dqn import DQNConfig
from ray.rllib.utils.test_utils import test_ckpt_restore
import os


# As we transition things to RLModule API the explore=False will get
# deprecated. For now, we will just not set it. The reason is that the RLModule
# API has forward_exploration() method that can be overriden if user needs to
# really turn of the stochasticity. This test in particular is robust to
# explore=None if we compare the mean of the distribution of actions for the
# same observation to be the same.
algorithms_and_configs = {
    "DQN": (
        DQNConfig()
        .env_runners(explore=False)
        .training(num_steps_sampled_before_learning_starts=0)
        .resources(num_gpus=int(os.environ.get("RLLIB_NUM_GPUS", "0")))
    ),
    "PPO": (
        # See the comment before the `algorithms_and_configs` dict.
        # explore is set to None for PPO in favor of RLModule API support.
        PPOConfig()
        .training(num_epochs=5, train_batch_size=1000)
        .env_runners(num_env_runners=2)
        .resources(num_gpus=int(os.environ.get("RLLIB_NUM_GPUS", "0")))
        .evaluation(
            evaluation_num_env_runners=1,
            evaluation_interval=1,
            evaluation_config=PPOConfig.overrides(
                # Define a (slightly different mapping function to test, whether eval
                # workers save their own version of this (and are restored properly
                # with this different mapping).
                # This is dummy logic; the point is to have a different source code
                # to be able to distinguish and compare to the default mapping fn used.
                policy_mapping_fn=lambda aid, episode, worker, **kw: (
                    "default_policy" if aid == "a0" else "default_policy"
                ),
            ),
        )
    ),
    "SAC": (
        SACConfig()
        .env_runners(explore=False)
        .training(num_steps_sampled_before_learning_starts=0)
        .resources(num_gpus=int(os.environ.get("RLLIB_NUM_GPUS", "0")))
    ),
}


class TestCheckpointRestorePPO(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init(num_cpus=6)

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_ppo_checkpoint_restore(self):
        test_ckpt_restore(
            algorithms_and_configs["PPO"],
            "CartPole-v1",
            eval_env_runner_group=True,
        )


class TestCheckpointRestoreOffPolicy(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init()

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_dqn_checkpoint_restore(self):
        test_ckpt_restore(
            algorithms_and_configs["DQN"],
            "CartPole-v1",
            replay_buffer=True,
        )

    def test_sac_checkpoint_restore(self):
        test_ckpt_restore(
            algorithms_and_configs["SAC"], "Pendulum-v1", replay_buffer=True
        )


if __name__ == "__main__":
    import pytest
    import sys

    # One can specify the specific TestCase class to run.
    # None for all unittest.TestCase classes in this file.
    class_ = sys.argv[1] if len(sys.argv) > 1 else None
    sys.exit(pytest.main(["-v", __file__ + ("" if class_ is None else "::" + class_)]))

import numpy as np
import unittest

import ray
from ray.rllib.algorithms.sac.sac import SACConfig
from ray.rllib.algorithms.sac.tests.test_sac import SimpleEnv
from ray.rllib.env.single_agent_env_runner import SingleAgentEnvRunner
from ray.rllib.utils.framework import try_import_torch

torch, _ = try_import_torch()


class TestSACWithNewStack(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        np.random.seed(42)
        torch.manual_seed(42)
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_simple_sac(self):
        config = (
            SACConfig()
            .environment(
                SimpleEnv,
                env_config={"simplex_actions": False},
            )
            .training(
                n_step=3,
                twin_q=True,
                initial_alpha=1.001,
                replay_buffer_config = {
                #"_enable_replay_buffer_api": True,
                    "type": "EpisodeReplayBuffer",
                    "capacity": 40000,
                },
                num_steps_sampled_before_learning_starts=0,
                store_buffer_in_checkpoints=True,
                train_batch_size=10,
                model={
                    "post_fcnet_hiddens": [],
                    "post_fcnet_activation": None,
                    "post_fcnet_weights_initializer": "xavier_normal_",
                    "post_fcnet_weights_initializer_config": {"gain": 0.01},
                }
            )
            .experimental(_enable_new_api_stack=True)
            .rollouts(
                num_rollout_workers=0,
                rollout_fragment_length=10,
                env_runner_cls=SingleAgentEnvRunner,
            )
        )
        num_iterations = 1

        algo = config.build()

        for _ in range(num_iterations):
            results = algo.train()

        print(results)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))

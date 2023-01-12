import gym
import unittest

import ray
from ray.rllib.core.rl_trainer.trainer_runner_config import TrainerRunnerConfig
from ray.rllib.core.testing.tf.bc_module import DiscreteBCTFModule
from ray.rllib.core.testing.tf.bc_rl_trainer import BCTfRLTrainer


class TestAlgorithmConfig(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init()

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_trainer_runner_build(self):

        env = gym.make("CartPole-v1")

        config = (
            TrainerRunnerConfig()
            .module(
                module_class=DiscreteBCTFModule,
                observation_space=env.observation_space,
                action_space=env.action_space,
                model_config={"hidden_dim": 32},
            )
            .trainer(
                trainer_class=BCTfRLTrainer,
            )
        )
        config = config.testing(True)
        config.build()


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))

import gym
import unittest

import ray

from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.core.rl_module.rl_module import SingleAgentRLModuleSpec
from ray.rllib.core.rl_trainer.trainer_runner_config import TrainerRunnerConfig
from ray.rllib.core.testing.tf.bc_module import DiscreteBCTFModule
from ray.rllib.core.testing.tf.bc_rl_trainer import BCTfRLTrainer
from ray.rllib.core.testing.utils import get_module_spec


class TestAlgorithmConfig(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init()

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_trainer_runner_build(self):
        """Tests whether the trainer_runner can be constructed and built."""

        env = gym.make("CartPole-v1")

        config = (
            TrainerRunnerConfig()
            .module(get_module_spec("tf", env))
            .trainer(
                trainer_class=BCTfRLTrainer,
            )
        )
        config.build()

    def test_trainer_runner_build_from_algorithm_config(self):
        """Tests whether we can build a trainer runner object from algorithm_config."""

        env = gym.make("CartPole-v1")

        config = (
            AlgorithmConfig()
            .rl_module(rl_module_class=DiscreteBCTFModule)
            .training(rl_trainer_class=BCTfRLTrainer)
            .training(model={"hidden_dim": 32})
        )
        config.freeze()
        runner_config = config.get_trainer_runner_config(
            SingleAgentRLModuleSpec(
                observation_space=env.observation_space, action_space=env.action_space
            )
        )
        runner_config.build()


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))

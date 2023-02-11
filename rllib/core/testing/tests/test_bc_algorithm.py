import gymnasium as gym
import unittest
import tensorflow as tf
import numpy as np

import ray

from ray.rllib.core.rl_module.rl_module import SingleAgentRLModuleSpec
from ray.rllib.core.rl_trainer.rl_trainer import RLTrainer, FrameworkHPs
from ray.rllib.core.testing.torch.bc_module import DiscreteBCTorchModule
from ray.rllib.core.testing.tf.bc_module import DiscreteBCTFModule
from ray.rllib.core.testing.tf.bc_rl_trainer import BCTfRLTrainer
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID
from ray.rllib.utils.test_utils import check, get_cartpole_dataset_reader
from ray.rllib.core.rl_trainer.scaling_config import TrainerScalingConfig

from ray.rllib.core.testing.bc_algorithm import BCConfigTest, BCAlgorithmTest
from ray.rllib.utils.test_utils import framework_iterator


class TestRLTrainer(unittest.TestCase):
    @classmethod
    def setUp(cls) -> None:
        ray.init()

    @classmethod
    def tearDown(cls) -> None:
        ray.shutdown()

    def test_bc_algorithm(self):

        config = (
            BCConfigTest()
            .rl_module(_enable_rl_module_api=True)
            .training(_enable_rl_trainer_api=True, model={"fcnet_hiddens": [32, 32]})
        )

        # TODO (Kourosh): Add tf2 support
        for fw in framework_iterator(config, frameworks=("torch")):
            algo = config.build(env="CartPole-v1")
            policy = algo.get_policy()
            rl_module = policy.model

            if fw == "torch":
                assert isinstance(rl_module, DiscreteBCTorchModule)
            elif fw == "tf":
                assert isinstance(rl_module, DiscreteBCTFModule)

    def test_bc_algorithm_w_custom_marl_module(self):
        pass


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))

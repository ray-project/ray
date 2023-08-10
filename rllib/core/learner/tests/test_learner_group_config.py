import gymnasium as gym
import unittest

import ray

from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.core.rl_module.rl_module import SingleAgentRLModuleSpec
from ray.rllib.core.learner.learner_group_config import LearnerGroupConfig
from ray.rllib.core.testing.tf.bc_module import DiscreteBCTFModule
from ray.rllib.core.testing.tf.bc_learner import BCTfLearner
from ray.rllib.core.testing.utils import get_module_spec


class TestAlgorithmConfig(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init()

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_learner_group_build(self):
        """Tests whether the learner_group can be constructed and built."""

        env = gym.make("CartPole-v1")

        config = (
            LearnerGroupConfig()
            .module(get_module_spec("tf2", env))
            .learner(
                learner_class=BCTfLearner,
            )
        )
        config.build()

    def test_learner_group_build_from_algorithm_config(self):
        """Tests whether we can build a learner_groupobject from algorithm_config."""

        env = gym.make("CartPole-v1")

        config = AlgorithmConfig().training(learner_class=BCTfLearner)
        config.freeze()
        learner_group_config = config.get_learner_group_config(
            SingleAgentRLModuleSpec(
                module_class=DiscreteBCTFModule,
                observation_space=env.observation_space,
                action_space=env.action_space,
                model_config_dict={"fcnet_hiddens": [32]},
            )
        )
        learner_group_config.build()


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))

import numpy as np
import unittest

import ray
import ray.rllib.agents.dqn as dqn
from ray.rllib.evaluation.postprocessing import Postprocessing
from ray.rllib.models.tf.tf_action_dist import Categorical
from ray.rllib.models.torch.torch_action_dist import TorchCategorical
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils import check, fc


class TestPDQN(unittest.TestCase):

    ray.init()

    def test_dqn_compilation(self):
        """Test whether a DQNTrainer can be built with both frameworks."""
        config = dqn.DEFAULT_CONFIG.copy()
        config["num_workers"] = 0  # Run locally.

        # tf.
        trainer = dqn.DQNTrainer(config=config, env="CartPole-v0")

        num_iterations = 2
        for i in range(num_iterations):
            trainer.train()

        # Torch.
        config["use_pytorch"] = True
        trainer = pg.PGTrainer(config=config, env="CartPole-v0")
        for i in range(num_iterations):
            trainer.train()

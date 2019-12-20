from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

#import numpy as np
import unittest

import ray
import ray.rllib.agents.ppo as ppo
#from ray.rllib.evaluation.postprocessing import Postprocessing
#from ray.rllib.models.tf.tf_action_dist import Categorical
#from ray.rllib.models.torch.torch_action_dist import TorchCategorical
#from ray.rllib.policy.sample_batch import SampleBatch
#from ray.rllib.utils import check, fc


class TestPPOFunctionality(unittest.TestCase):

    ray.init()

    def test_ppo_compilation(self):
        """Test whether a PPOTrainer can be built with both backends."""
        config = ppo.DEFAULT_CONFIG.copy()
        config["num_workers"] = 0  # Run locally.

        # tf.
        trainer = ppo.PPOTrainer(config=config, env="CartPole-v0")

        num_iterations = 2
        for i in range(num_iterations):
            trainer.train()

        # Torch.
        config["use_pytorch"] = True
        config["simple_optimizer"] = True
        trainer = ppo.PPOTrainer(config=config, env="CartPole-v0")
        for i in range(num_iterations):
            trainer.train()

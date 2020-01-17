import numpy as np
import unittest

import ray
from ray.rllib.agents.impala.vtrace_policy import BEHAVIOR_LOGITS
import ray.rllib.agents.ppo.appo as appo
from ray.rllib.agents.ppo.ppo_tf_policy import postprocess_ppo_gae as \
    postprocess_ppo_gae_tf, ppo_surrogate_loss as ppo_surrogate_loss_tf
from ray.rllib.agents.ppo.ppo_torch_policy import postprocess_ppo_gae as \
    postprocess_ppo_gae_torch, ppo_surrogate_loss as ppo_surrogate_loss_torch
from ray.rllib.evaluation.postprocessing import Postprocessing
from ray.rllib.models.tf.tf_action_dist import Categorical
from ray.rllib.models.torch.torch_modelv2 import TorchModelV2
from ray.rllib.models.torch.torch_action_dist import TorchCategorical
from ray.rllib.policy.policy import ACTION_LOGP
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.numpy import fc
from ray.rllib.utils.test_utils import check


class TestAPPO(unittest.TestCase):

    ray.init()

    def test_appo_compilation(self):
        """Test whether an APPOTrainer can be built with both frameworks."""
        config = appo.DEFAULT_CONFIG.copy()
        config["num_workers"] = 2  # Run with 2 workers (async optimizer).

        # tf.
        trainer = appo.APPOTrainer(config=config, env="CartPole-v0")

        num_iterations = 2
        for i in range(num_iterations):
            trainer.train()

        # Torch.
        config["use_pytorch"] = True
        #config["simple_optimizer"] = True
        trainer = appo.APPOTrainer(config=config, env="CartPole-v0")
        for i in range(num_iterations):
            trainer.train()

    def test_appo_loss_function(self):
        return
        # TODO:

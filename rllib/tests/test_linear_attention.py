import numpy as np
import copy
import pickle
import unittest

import ray
from ray.rllib.examples.env.stateless_cartpole import StatelessCartPole
from ray.rllib.utils.test_utils import check_learning_achieved

class TestLinearAttention(unittest.TestCase):
    def setUp(self):
        self.config = {
                "num_workers": 0,
                "num_gpus": 0,
                "vf_loss_coeff": 0.01,
                "env": StatelessCartPole,
                # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
                "model": {
                    "use_linear_attention": True,
                    "linear_attn_hidden_size": 256,
                    "linear_attn_use_embedding": False,
                },
                "framework": "torch",
        }
        self.stop = {
            "training_iteration": 200,
            "timesteps_total": 100_000,
            "episode_reward_mean": 150.0,
        }
        ray.init(num_cpus=0)

    def test_learning_cartpole(self):
        results = ray.tune.run("IMPALA", config=self.config, stop=self.stop)
        check_learning_achieved(results)
        

    def test_learning_cartpole_embedded(self):
        config = copy.deepcopy(self.config)
        config["model"]["linear_attn_use_embedding"] = True
        results = ray.tune.run("IMPALA", config=config, stop=self.stop)
        check_learning_achieved(results)

    def tearDown(self) -> None:
        ray.shutdown()
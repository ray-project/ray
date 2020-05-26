import unittest

from ray import tune
import ray.rllib.agents.pg as pg
from ray.rllib.examples.env.stateless_cartpole import StatelessCartPole
from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.models.tf.attention_net import GTrXLNet


class TestAttentionNetLearning(unittest.TestCase):
    def test_pg_attention_net_learning(self):
        ModelCatalog.register_custom_model("attention_net", GTrXLNet)
        config = {
            "env": StatelessCartPole,
            "model": {
                "custom_model": "attention_net",
                "max_seq_len": 50,
                "custom_model_config": {
                    "num_transformer_units": 1,
                    "attn_dim": 64,
                    "num_heads": 2,
                    "memory_tau": 50,
                    "head_dim": 32,
                    "ff_hidden_dim": 32,
                },
            },
            "num_envs_per_worker": 5,  # test with vectorization on
            # "framework": "tf",
        }
        stop = {
            "episode_reward_mean": 180.0
        }
        tune.run("PG", config=config, stop=stop)


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))

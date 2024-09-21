from gymnasium.spaces import Box, Dict, Discrete, MultiDiscrete, Tuple
import unittest

import ray
from ray import air, tune
from ray.air.constants import TRAINING_ITERATION
from ray.rllib.examples.envs.classes.random_env import RandomEnv
from ray.rllib.examples.envs.classes.stateless_cartpole import StatelessCartPole
from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.models.tf.attention_net import GTrXLNet
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    EPISODE_RETURN_MEAN,
    NUM_ENV_STEPS_SAMPLED_LIFETIME,
)


class TestAttentionNets(unittest.TestCase):

    config = {
        "env": StatelessCartPole,
        "gamma": 0.99,
        "num_envs_per_env_runner": 20,
        "framework": "tf",
    }

    stop = {
        f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": 150.0,
        f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}": 5000000,
    }

    @classmethod
    def setUpClass(cls) -> None:
        ray.init(num_cpus=5)

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_attention_nets_w_prev_actions_and_prev_rewards(self):
        """Tests attention prev-a/r input insertions using complex actions."""
        config = {
            "env": RandomEnv,
            "env_config": {
                "config": {
                    "action_space": Dict(
                        {
                            "a": Box(-1.0, 1.0, ()),
                            "b": Box(-1.0, 1.0, (2,)),
                            "c": Tuple(
                                [
                                    Discrete(2),
                                    MultiDiscrete([2, 3]),
                                    Box(-1.0, 1.0, (3,)),
                                ]
                            ),
                        }
                    ),
                },
            },
            # Need to set this to True to enable complex (prev.) actions
            # as inputs to the attention net.
            "_disable_action_flattening": True,
            "model": {
                "fcnet_hiddens": [10],
                "use_attention": True,
                "attention_dim": 16,
                "attention_use_n_prev_actions": 3,
                "attention_use_n_prev_rewards": 2,
            },
            "num_epochs": 1,
            "train_batch_size": 200,
            "minibatch_size": 50,
            "rollout_fragment_length": 100,
            "num_env_runners": 1,
        }
        tune.Tuner(
            "PPO",
            param_space=config,
            run_config=air.RunConfig(stop={TRAINING_ITERATION: 1}, verbose=1),
        ).fit()

    def test_ppo_attention_net_learning(self):
        ModelCatalog.register_custom_model("attention_net", GTrXLNet)
        config = dict(
            self.config,
            **{
                "num_env_runners": 0,
                "entropy_coeff": 0.001,
                "vf_loss_coeff": 1e-5,
                "num_epochs": 5,
                "model": {
                    "custom_model": "attention_net",
                    "max_seq_len": 10,
                    "custom_model_config": {
                        "num_transformer_units": 1,
                        "attention_dim": 32,
                        "num_heads": 1,
                        "memory_inference": 5,
                        "memory_training": 5,
                        "head_dim": 32,
                        "position_wise_mlp_dim": 32,
                    },
                },
            },
        )
        tune.Tuner(
            "PPO",
            param_space=config,
            run_config=air.RunConfig(stop=self.stop, verbose=1),
        ).fit()

    # TODO: (sven) causes memory failures/timeouts on Travis.
    #  Re-enable this once we have fast attention in master branch.
    def test_impala_attention_net_learning(self):
        return
        # ModelCatalog.register_custom_model("attention_net", GTrXLNet)
        # config = dict(
        #    self.config, **{
        #        "num_env_runners": 4,
        #        "num_gpus": 0,
        #        "entropy_coeff": 0.01,
        #        "vf_loss_coeff": 0.001,
        #        "lr": 0.0008,
        #        "model": {
        #            "custom_model": "attention_net",
        #            "max_seq_len": 65,
        #            "custom_model_config": {
        #                "num_transformer_units": 1,
        #                "attention_dim": 64,
        #                "num_heads": 1,
        #                "memory_inference": 10,
        #                "memory_training": 10,
        #                "head_dim": 32,
        #                "position_wise_mlp_dim": 32,
        #            },
        #        },
        #    })
        # tune.Tuner(
        #     "IMPALA",
        #     param_space=config,
        #     run_config=air.RunConfig(stop=self.stop, verbose=1),
        # ).fit()


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))

import os
import sys
import unittest

import pytest
import ray
from ray import tune
from ray.job_config import JobConfig
from ray.rllib.agents import ppo
from ray.rllib.examples.env.stateless_cartpole import StatelessCartPole
from ray.rllib.utils.test_utils import check_learning_achieved
from ray.util.client.ray_client_helpers import ray_start_client_server


class TestRayClient(unittest.TestCase):
    def test_connection(self):
        with ray_start_client_server():
            assert ray.util.client.ray.is_connected()
        assert ray.util.client.ray.is_connected() is False

    def test_custom_train_fn(self):
        with ray_start_client_server():
            assert ray.util.client.ray.is_connected()

            config = {
                "lr": 0.01,
                # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
                "num_gpus": int(os.environ.get("RLLIB_NUM_GPUS", "0")),
                "num_workers": 0,
                "framework": "tf",
            }
            resources = ppo.PPOTrainer.default_resource_request(config)
            from ray.rllib.examples.custom_train_fn import my_train_fn
            tune.run(my_train_fn, resources_per_trial=resources, config=config)

    def test_cartpole_lstm(self):
        with ray_start_client_server():
            assert ray.util.client.ray.is_connected()

            config = dict(
                {
                    "num_sgd_iter": 5,
                    "model": {
                        "vf_share_layers": True,
                    },
                    "vf_loss_coeff": 0.0001,
                },
                **{
                    "env": StatelessCartPole,
                    # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
                    "num_gpus": int(os.environ.get("RLLIB_NUM_GPUS", "0")),
                    "model": {
                        "use_lstm": True,
                        "lstm_cell_size": 256,
                        "lstm_use_prev_action": None,
                        "lstm_use_prev_reward": None,
                    },
                    "framework": "tf",
                    # Run with tracing enabled for tfe/tf2?
                    "eager_tracing": None,
                })

            stop = {
                "training_iteration": 200,
                "timesteps_total": 100000,
                "episode_reward_mean": 150.0,
            }

            results = tune.run("PPO", config=config, stop=stop, verbose=2)
            check_learning_achieved(results, 150.0)

    def test_custom_experiment(self):
        def ray_connect_handler(job_config: JobConfig = None):
            ray.init(num_cpus=3)

        with ray_start_client_server(ray_connect_handler=ray_connect_handler):
            assert ray.util.client.ray.is_connected()

            config = ppo.DEFAULT_CONFIG.copy()
            config["train-iterations"] = 10
            config["env"] = "CartPole-v0"

            from ray.rllib.examples.custom_experiment import experiment
            tune.run(
                experiment,
                config=config,
                resources_per_trial=ppo.PPOTrainer.default_resource_request(
                    config))


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))

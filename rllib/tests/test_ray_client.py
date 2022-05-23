import os
import sys
import unittest

import pytest
import ray
from ray import tune
from ray.rllib.agents import ppo
from ray.rllib.examples.env.stateless_cartpole import StatelessCartPole
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
                # Special flag signalling `my_train_fn` how many iters to do.
                "train-iterations": 2,
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

            config = {
                "env": StatelessCartPole,
            }

            stop = {
                "training_iteration": 3,
            }

            tune.run("PPO", config=config, stop=stop, verbose=2)

    def test_custom_experiment(self):

        with ray_start_client_server(ray_init_kwargs={"num_cpus": 3}):
            assert ray.util.client.ray.is_connected()

            config = ppo.DEFAULT_CONFIG.copy()
            # Special flag signalling `experiment` how many iters to do.
            config["train-iterations"] = 2
            config["env"] = "CartPole-v0"

            from ray.rllib.examples.custom_experiment import experiment

            tune.run(
                experiment,
                config=config,
                resources_per_trial=ppo.PPOTrainer.default_resource_request(config),
            )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))

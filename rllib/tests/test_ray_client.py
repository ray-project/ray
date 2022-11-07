import os
import sys
import unittest

import pytest
import ray
from ray import air
from ray import tune
import ray.rllib.algorithms.ppo as ppo
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
            resources = ppo.PPO.default_resource_request(config)
            from ray.rllib.examples.custom_train_fn import my_train_fn

            tune.Tuner(
                tune.with_resources(my_train_fn, resources),
                param_space=config,
            ).fit()

    def test_cartpole_lstm(self):
        with ray_start_client_server():
            assert ray.util.client.ray.is_connected()

            config = {
                "env": StatelessCartPole,
            }

            stop = {
                "training_iteration": 3,
            }

            tune.Tuner(
                "PPO",
                param_space=config,
                run_config=air.RunConfig(stop=stop, verbose=2),
            ).fit()

    def test_custom_experiment(self):

        with ray_start_client_server(ray_init_kwargs={"num_cpus": 3}):
            assert ray.util.client.ray.is_connected()

            config = ppo.DEFAULT_CONFIG.copy()
            # Special flag signalling `experiment` how many iters to do.
            config["train-iterations"] = 2
            config["env"] = "CartPole-v1"

            from ray.rllib.examples.custom_experiment import experiment

            tune.Tuner(
                tune.with_resources(
                    experiment, ppo.PPO.default_resource_request(config)
                ),
                param_space=config,
            ).fit()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))

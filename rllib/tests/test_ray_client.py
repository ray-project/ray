import sys

import pytest

from ray.rllib.algorithms import dqn
from ray.util.client.ray_client_helpers import ray_start_client_server
from ray._private.client_mode_hook import enable_client_mode, client_mode_should_convert


def test_basic_dqn():
    with ray_start_client_server():
        # Need to enable this for client APIs to be used.
        with enable_client_mode():
            # Confirming mode hook is enabled.
            assert client_mode_should_convert()
            config = (
                dqn.DQNConfig()
                .environment("CartPole-v1")
                .env_runners(num_env_runners=0, compress_observations=True)
            )
            trainer = config.build()
            for i in range(2):
                trainer.train()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))

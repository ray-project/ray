import sys

import pytest

from ray import tune
from ray.util.client.ray_client_helpers import ray_start_client_server
from ray._private.client_mode_hook import enable_client_mode, client_mode_should_convert


@pytest.mark.skip(reason="KV store is not working properly.")
def test_rllib_integration(ray_start_regular):
    with ray_start_client_server():
        import ray.rllib.algorithms.dqn as dqn

        # Confirming the behavior of this context manager.
        # (Client mode hook not yet enabled.)
        assert not client_mode_should_convert()
        # Need to enable this for client APIs to be used.
        with enable_client_mode():
            # Confirming mode hook is enabled.
            assert client_mode_should_convert()

            config = (
                dqn.DQNConfig().environment("CartPole-v1")
                # Run locally.
                # Test with compression.
                .rollouts(num_rollout_workers=0, compress_observations=True)
            )
            num_iterations = 2
            trainer = config.build()
            rw = trainer.workers.local_worker()
            for i in range(num_iterations):
                sb = rw.sample()
                assert sb.count == config["rollout_fragment_length"]
                trainer.train()


def test_rllib_integration_tune(ray_start_regular):
    with ray_start_client_server():
        # Confirming the behavior of this context manager.
        # (Client mode hook not yet enabled.)
        assert not client_mode_should_convert()
        # Need to enable this for client APIs to be used.
        with enable_client_mode():
            # Confirming mode hook is enabled.
            assert client_mode_should_convert()
            tune.run(
                "DQN", config={"env": "CartPole-v1"}, stop={"training_iteration": 2}
            )


@pytest.mark.asyncio
async def test_serve_handle(ray_start_regular):
    with ray_start_client_server():
        from ray import serve

        with enable_client_mode():
            serve.start()

            @serve.deployment
            def hello():
                return "hello"

            handle = serve.run(hello.bind()).options(use_new_handle_api=True)
            assert await handle.remote() == "hello"


if __name__ == "__main__":
    import os

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))

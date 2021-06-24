import sys

import pytest

from ray.rllib.examples import rock_paper_scissors_multiagent
from ray.util.client.ray_client_helpers import ray_start_client_server
from ray._private.client_mode_hook import _explicitly_enable_client_mode,\
    client_mode_should_convert


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
def test_rllib_integration(ray_start_regular_shared):
    with ray_start_client_server():
        # Confirming the behavior of this context manager.
        # (Client mode hook not yet enabled.)
        assert not client_mode_should_convert()
        # Need to enable this for client APIs to be used.
        _explicitly_enable_client_mode()
        # Confirming mode hook is enabled.
        assert client_mode_should_convert()

        rock_paper_scissors_multiagent.main()


@pytest.mark.asyncio
async def test_serve_handle(ray_start_regular_shared):
    with ray_start_client_server() as ray:
        from ray import serve
        _explicitly_enable_client_mode()
        serve.start()

        @serve.deployment
        def hello():
            return "hello"

        hello.deploy()
        handle = hello.get_handle()
        assert ray.get(handle.remote()) == "hello"
        assert await handle.remote() == "hello"


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))

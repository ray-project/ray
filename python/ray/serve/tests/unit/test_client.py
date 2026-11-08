import asyncio
import sys
from typing import Awaitable, Callable
from unittest import mock

import pytest

from ray.serve._private.client import ServeControllerClient
from ray.serve._private.common import DeploymentID
from ray.serve.exceptions import RayServeException


async def _shutdown_sync(client: ServeControllerClient) -> None:
    client.shutdown()


async def _shutdown_async(client: ServeControllerClient) -> None:
    await client.shutdown_async()


SHUTDOWNS = pytest.mark.parametrize(
    "shutdown", [_shutdown_sync, _shutdown_async], ids=["sync", "async"]
)


async def _get_handle_async_across_shutdown(
    shutdown: Callable[[ServeControllerClient], Awaitable[None]],
) -> tuple[ServeControllerClient, "asyncio.Task"]:
    """Shut `client` down while `get_handle_async` awaits the controller.

    Returns the client and the finished `get_handle_async` task, so tests can
    check what that call did once it resumed after the shutdown.
    """
    controller_called = asyncio.Event()
    deployment_ids = asyncio.get_running_loop().create_future()

    def list_deployment_ids() -> "asyncio.Future":
        controller_called.set()
        return deployment_ids

    # Bypass `__init__`, which blocks on the controller for its HTTP config.
    client = object.__new__(ServeControllerClient)
    client._controller = mock.Mock()
    client._controller.list_deployment_ids.remote.side_effect = list_deployment_ids
    client._shutdown = False
    client._is_shutting_down = False
    client.handle_cache = {}
    client._evicted_handle_keys = set()

    get_handle = asyncio.create_task(client.get_handle_async("f", "app"))
    await controller_called.wait()

    await shutdown(client)
    deployment_ids.set_result([DeploymentID(name="f", app_name="app")])
    await asyncio.wait([get_handle])

    return client, get_handle


@SHUTDOWNS
async def test_get_handle_async_raises_when_shutdown_starts_mid_call(shutdown):
    _, get_handle = await _get_handle_async_across_shutdown(shutdown)

    with pytest.raises(RayServeException, match="shutting down"):
        get_handle.result()


@SHUTDOWNS
async def test_get_handle_async_does_not_cache_when_shutdown_starts_mid_call(
    shutdown,
):
    client, get_handle = await _get_handle_async_across_shutdown(shutdown)
    get_handle.exception()  # Retrieve it so asyncio doesn't log it as unhandled.

    assert client.handle_cache == {}


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))

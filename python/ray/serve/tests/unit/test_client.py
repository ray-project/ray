import asyncio
import sys
from typing import Awaitable, Callable, Optional
from unittest import mock

import pytest

from ray.serve._private.client import ServeControllerClient
from ray.serve._private.common import DeploymentID
from ray.serve.config import HTTPOptions
from ray.serve.exceptions import RayServeException

# Either refusal: which one a getter sees depends on whether the shutdown it
# raced has finished by the time it checks.
REFUSED = r"Client is shutting down\.|Client has already been shut down\."


def _make_client(controller: Optional[mock.Mock] = None) -> ServeControllerClient:
    """Build a client around a mock controller, as `from_controller` would."""
    return ServeControllerClient(
        controller or mock.Mock(),
        http_config=HTTPOptions(),
        root_url="http://serve.test:8123",
    )


async def _shutdown_sync(client: ServeControllerClient) -> None:
    client.shutdown()


async def _shutdown_async(client: ServeControllerClient) -> None:
    await client.shutdown_async()


SHUTDOWNS = pytest.mark.parametrize(
    "shutdown", [_shutdown_sync, _shutdown_async], ids=["sync", "async"]
)


async def _get_handle_sync(client: ServeControllerClient) -> object:
    return client.get_handle("f", "app")


async def _get_handle_async(client: ServeControllerClient) -> object:
    return await client.get_handle_async("f", "app")


GETTERS = pytest.mark.parametrize(
    "get_handle", [_get_handle_sync, _get_handle_async], ids=["sync", "async"]
)


class BlockingShutdownHandle:
    """Stands in for a cached handle whose `shutdown_async` waits on `release`.

    Holds `ServeControllerClient.shutdown_async` partway through, so a test can
    act on the client while it is shutting down.
    """

    def __init__(self) -> None:
        self.shutdown_started = asyncio.Event()
        self.release = asyncio.Event()

    async def shutdown_async(self) -> None:
        self.shutdown_started.set()
        await self.release.wait()


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

    controller = mock.Mock()
    controller.list_deployment_ids.remote.side_effect = list_deployment_ids
    client = _make_client(controller)

    get_handle = asyncio.create_task(client.get_handle_async("f", "app"))
    await controller_called.wait()

    await shutdown(client)
    deployment_ids.set_result([DeploymentID(name="f", app_name="app")])
    await asyncio.wait([get_handle])

    return client, get_handle


@SHUTDOWNS
async def test_get_handle_async_raises_when_shutdown_starts_mid_call(shutdown):
    _, get_handle = await _get_handle_async_across_shutdown(shutdown)

    with pytest.raises(RayServeException, match=REFUSED):
        get_handle.result()


@SHUTDOWNS
async def test_get_handle_async_does_not_cache_when_shutdown_starts_mid_call(
    shutdown,
):
    client, get_handle = await _get_handle_async_across_shutdown(shutdown)
    get_handle.exception()  # Retrieve it so asyncio doesn't log it as unhandled.

    assert client.handle_cache == {}


@GETTERS
async def test_cached_handle_is_refused_while_shutting_down(get_handle):
    client = _make_client()
    cached = BlockingShutdownHandle()
    client.handle_cache[("f", "app", True)] = cached
    shutdown = asyncio.create_task(client.shutdown_async())
    await cached.shutdown_started.wait()

    try:
        with pytest.raises(RayServeException, match=r"Client is shutting down\."):
            await get_handle(client)
    finally:
        cached.release.set()
        await shutdown


@SHUTDOWNS
@GETTERS
async def test_handles_are_refused_after_shutdown_without_ray(shutdown, get_handle):
    client = _make_client()

    await shutdown(client)

    with pytest.raises(RayServeException, match=r"Client has already been shut down\."):
        await get_handle(client)


@GETTERS
async def test_handles_stay_refused_after_shutdown_fails_partway(get_handle):
    client = _make_client()
    cached = BlockingShutdownHandle()
    client.handle_cache[("f", "app", True)] = cached
    shutdown = asyncio.create_task(client.shutdown_async())
    await cached.shutdown_started.wait()

    shutdown.cancel()
    await asyncio.wait([shutdown])

    with pytest.raises(RayServeException, match=r"Client is shutting down\."):
        await get_handle(client)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))

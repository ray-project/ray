# Note: asyncio is only compatible with Python 3

import asyncio

import ray
from ray.experimental.async_plasma import (
    RayletEventProtocol, RayletEventHandler, RayletEventTransport)

handler = None
transport = None
protocol = None


def init():
    """Initialize the async APIs."""

    global handler, transport, protocol
    if handler is None:
        worker = ray.worker.global_worker
        loop = asyncio.get_event_loop()
        handler = RayletEventHandler(loop, worker)
        protocol = RayletEventProtocol(worker.plasma_client, handler)
        socket_fd = worker.raylet_client.get_native_event_socket_handle()
        transport = RayletEventTransport(loop, socket_fd, protocol)


def as_future(object_id, wait_only=False):
    """Turn an object_id into a Future object.

    Args:
        object_id: A Ray object_id.
        wait_only (bool): If true, the future will not fetch the object,
            it will return the original ObjectID instead.
    Returns:
        PlasmaObjectFuture: A future object that waits the object_id.
    """
    if handler is None:
        init()
    return handler.as_future(object_id, wait_only)


def shutdown():
    """Manually shutdown the async API.

    Cancels all related tasks and all the socket transportation.
    """
    global handler, transport, protocol
    if handler is not None:
        handler.close()
        transport.close()
        handler = None
        transport = None
        protocol = None

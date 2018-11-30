from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import asyncio
import ray
from ray.experimental.async_plasma import PlasmaProtocol, PlasmaEventHandler

handler: PlasmaEventHandler = None
transport = None
protocol = None


async def init():
    global handler, transport, protocol
    if handler is None:
        worker = ray.worker.global_worker
        loop = asyncio.get_event_loop()
        worker.plasma_client.subscribe()
        rsock = worker.plasma_client.get_notification_socket()
        handler = PlasmaEventHandler(loop, worker)
        transport, protocol = await loop.create_connection(
            lambda: PlasmaProtocol(loop, worker.plasma_client, handler),
            sock=rsock)


def sync_init():
    loop = asyncio.get_event_loop()
    if loop.is_running():
        raise Exception(
            "You cannot initialize async_api when the eventloop"
            " is running. Try to initialize it before you run the "
            "eventloop or use the async `init()` instead.")
    else:
        # Initialize synchronously.
        asyncio.get_event_loop().run_until_complete(init())


def as_future(object_id):
    """Turn an object_id into a Future object.

    Args:
        object_id: A Ray object_id.

    Returns:
        PlasmaObjectFuture: A future object that waits the object_id.
    """
    if handler is None:
        sync_init()
    return handler.as_future(object_id)


def shutdown():
    """Manually shutdown the async API.
    Cancel all related tasks and all the socket transportation."""
    global handler, transport, protocol
    if handler is not None:
        handler.close()
        transport.close()
        handler = None
        transport = None
        protocol = None

# Note: asyncio is only compatible with Python 3

import asyncio

import ray
from ray.experimental.async_plasma import PlasmaProtocol, PlasmaEventHandler
from ray.services import logger

handler = None
transport = None
protocol = None


async def _async_init():
    global handler, transport, protocol
    if handler is None:
        worker = ray.worker.global_worker
        loop = asyncio.get_event_loop()
        worker.plasma_client.subscribe()
        rsock = worker.plasma_client.get_notification_socket()
        handler = PlasmaEventHandler(loop, worker)
        transport, protocol = await loop.create_connection(
            lambda: PlasmaProtocol(worker.plasma_client, handler), sock=rsock)
        logger.debug("AsyncPlasma Connection Created!")


def init():
    """
    Initialize synchronously.
    """
    assert ray.is_initialized(), "Please call ray.init before async_api.init"

    loop = asyncio.get_event_loop()
    if loop.is_running():
        asyncio.ensure_future(_async_init())
    else:
        asyncio.get_event_loop().run_until_complete(_async_init())


def as_future(object_id):
    """Turn an object_id into a Future object.

    Args:
        object_id: A Ray object_id.

    Returns:
        PlasmaObjectFuture: A future object that waits the object_id.
    """
    if handler is None:
        init()
    return handler.as_future(object_id)


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

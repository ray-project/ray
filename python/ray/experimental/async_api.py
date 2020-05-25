import asyncio

import ray
from ray.experimental.async_plasma import PlasmaEventHandler
from ray.services import logger

handler = None


def init():
    """Initialize plasma event handlers for asyncio support."""
    assert ray.is_initialized(), "Please call ray.init before async_api.init"

    global handler
    if handler is None:
        worker = ray.worker.global_worker
        loop = asyncio.get_event_loop()
        handler = PlasmaEventHandler(loop, worker)
        worker.core_worker.set_plasma_added_callback(handler)
        logger.debug("AsyncPlasma Connection Created!")


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
    global handler
    if handler is not None:
        handler.close()
        handler = None

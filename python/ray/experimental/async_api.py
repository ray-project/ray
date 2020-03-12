# Note: asyncio is only compatible with Python 3

import asyncio
import threading

import ray
from ray.experimental.async_plasma import PlasmaEventHandler
from ray.services import logger

handler = None


async def _async_init():
    global handler
    if handler is None:
        worker = ray.worker.global_worker
        loop = asyncio.get_event_loop()
        handler = PlasmaEventHandler(loop, worker)
        worker.core_worker.set_plasma_added_callback(handler)
        logger.debug("AsyncPlasma Connection Created!")


def init():
    """
    Initialize synchronously.
    """
    assert ray.is_initialized(), "Please call ray.init before async_api.init"

    # Noop when handler is set.
    if handler is not None:
        return

    loop = asyncio.get_event_loop()
    if loop.is_running():
        if loop._thread_id != threading.get_ident():
            # If the loop is runing outside current thread, we actually need
            # to do this to make sure the context is initialized.
            asyncio.run_coroutine_threadsafe(_async_init(), loop=loop)
        else:
            async_init_done = asyncio.get_event_loop().create_task(
                _async_init())
            # Block until the async init finishes.
            async_init_done.done()
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
    global handler
    if handler is not None:
        handler.close()
        handler = None

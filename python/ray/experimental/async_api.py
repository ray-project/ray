# Note: asyncio is only compatible with Python 3

import asyncio
import functools
import threading

import pyarrow.plasma as plasma

import ray
from ray.experimental.async_plasma import PlasmaProtocol, PlasmaEventHandler
from ray.services import logger

handler = None
transport = None
protocol = None


class _ThreadSafeProxy(object):
    """This class is used to create a thread-safe proxy for a given object.
        Every method call will be guarded with a lock.

    Attributes:
        orig_obj (object): the original object.
        lock (threading.Lock): the lock object.
        _wrapper_cache (dict): a cache from original object's methods to
            the proxy methods.
    """

    def __init__(self, orig_obj, lock):
        self.orig_obj = orig_obj
        self.lock = lock
        self._wrapper_cache = {}

    def __getattr__(self, attr):
        orig_attr = getattr(self.orig_obj, attr)
        if not callable(orig_attr):
            # If the original attr is a field, just return it.
            return orig_attr
        else:
            # If the orginal attr is a method,
            # return a wrapper that guards the original method with a lock.
            wrapper = self._wrapper_cache.get(attr)
            if wrapper is None:

                @functools.wraps(orig_attr)
                def _wrapper(*args, **kwargs):
                    with self.lock:
                        return orig_attr(*args, **kwargs)

                self._wrapper_cache[attr] = _wrapper
                wrapper = _wrapper
            return wrapper


def thread_safe_client(client, lock=None):
    """Create a thread-safe proxy which locks every method call
    for the given client.
    Args:
        client: the client object to be guarded.
        lock: the lock object that will be used to lock client's methods.
            If None, a new lock will be used.
    Returns:
        A thread-safe proxy for the given client.
    """
    if lock is None:
        lock = threading.Lock()
    return _ThreadSafeProxy(client, lock)


async def _async_init():
    global handler, transport, protocol
    if handler is None:
        worker = ray.worker.global_worker
        plasma_client = thread_safe_client(
            plasma.connect(worker.node.plasma_store_socket_name, None, 0, 300))
        loop = asyncio.get_event_loop()
        plasma_client.subscribe()
        rsock = plasma_client.get_notification_socket()
        handler = PlasmaEventHandler(loop, worker)
        transport, protocol = await loop.create_connection(
            lambda: PlasmaProtocol(plasma_client, handler), sock=rsock)
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

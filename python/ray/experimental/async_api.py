from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray
from ray.experimental.plasma_eventloop import (
    PlasmaEpoll, PlasmaPoll, PlasmaSelectorEventLoop, PlasmaFutureGroup,
    RayAsyncParamsType)

global_worker = ray.worker.global_worker
eventloop: PlasmaSelectorEventLoop = None


def cleanup():
    global eventloop
    if eventloop is not None:
        eventloop.close()
        eventloop = None


def _init_eventloop(selector_name='poll'):
    global eventloop
    if eventloop is None:
        if selector_name == 'poll':
            selector = PlasmaPoll(global_worker)
        elif selector_name == 'epoll':
            selector = PlasmaEpoll(global_worker)
        else:
            raise Exception("Unknown selector name '%s'" % selector_name)
        eventloop = PlasmaSelectorEventLoop(selector, worker=global_worker)


def set_debug(enabled):
    if eventloop is None:
        _init_eventloop()
    eventloop.set_debug(enabled)


def run_until_complete(future):
    if eventloop is None:
        _init_eventloop()
    return eventloop.run_until_complete(future)


def create_group(return_exceptions=False,
                 keep_duplicated=True,
                 worker=global_worker) -> PlasmaFutureGroup:
    """This function creates an instance of `PlasmaFutureGroup`.

    Args:
        return_exceptions(bool): If true, return exceptions as results
            instead of raising them.
        keep_duplicated(bool): If true, an future can be added multiple times.
        worker: Ray's worker.

    Returns:
        A `PlasmaFutureGroup` instance.
    """

    worker.check_connected()
    if eventloop is None:
        _init_eventloop()
    return PlasmaFutureGroup(
        eventloop,
        return_exceptions=return_exceptions,
        keep_duplicated=keep_duplicated)


async def get(object_ids: RayAsyncParamsType, worker=global_worker):
    """Get a remote object or a list of remote objects from the object store.

    This method blocks until the object corresponding to the object ID is
    available in the local object store. If this object is not in the local
    object store, it will be shipped from an object store that has it (once the
    object has been created). If object_ids is a list, then the objects
    corresponding to each object in the list will be returned.

    Args:
        object_ids (RayAsyncParamsType): Object ID of the object to get
            or a list of object IDs to get.
            Futures & coroutines containing IDs is also acceptable.

    Returns:
        A Python object or a list of Python objects.
    """

    worker.check_connected()
    if eventloop is None:
        _init_eventloop()
    return await eventloop.get(object_ids)


async def wait(object_ids: RayAsyncParamsType,
               num_returns=1,
               timeout=None,
               worker=global_worker):
    """Return a list of IDs that are ready and a list of IDs that are not.

    If timeout is set, the function returns either when the requested number of
    IDs are ready or when the timeout is reached, whichever occurs first. If it
    is not set, the function simply waits until that number of objects is ready
    and returns that exact number of object_ids.

    This method returns two lists. The first list consists of object IDs that
    correspond to objects that are stored in the object store. The second list
    corresponds to the rest of the object IDs (which may or may not be ready).

    Ordering of the input list of object IDs is preserved: if A precedes B in
    the input list, and both are in the ready list, then A will precede B in
    the ready list. This also holds true if A and B are both in the remaining
    list.

    Args:
        object_ids (RayAsyncParamsType): List of object IDs
            (also futures & coroutines contain an ID) for objects that may or
            may not be ready. Note that these IDs must be unique.
        num_returns (int): The number of object IDs that should be returned.
        timeout (int): The maximum amount of time in milliseconds to wait
            before returning.

    Returns:
        A list of object IDs that are ready and a list of the remaining object
            IDs. Because `ray.experimental.async_api.wait` supports
            futures and coroutines as its input,
            it could happen that a passing in future/coroutine fails to return
            an ObjectID before timeout.  In this case, we will return
            the pending inputs.
    """

    if isinstance(object_ids, ray.ObjectID):
        raise TypeError(
            "wait() expected a list of ObjectID, got a single ObjectID")

    if not isinstance(object_ids, list):
        raise TypeError("wait() expected a list of ObjectID, got {}".format(
            type(object_ids)))

    worker.check_connected()
    if eventloop is None:
        _init_eventloop()

    # TODO(rkn): This is a temporary workaround for
    # https://github.com/ray-project/ray/issues/997. However, it should be
    # fixed in Arrow instead of here.
    if len(object_ids) == 0:
        return [], []

    if len(object_ids) != len(set(object_ids)):
        raise Exception("Wait requires a list of unique object IDs.")
    if num_returns <= 0:
        raise Exception(
            "Invalid number of objects to return %d." % num_returns)
    if num_returns > len(object_ids):
        raise Exception("num_returns cannot be greater than the number "
                        "of objects provided to ray.wait.")

    # Convert milliseconds into seconds.
    timeout = timeout / 1000 if timeout is not None else 2**30

    return await eventloop.wait(
        object_ids, num_returns=num_returns, timeout=timeout)

import logging
import time
import sys

import ray.worker
from ray import ObjectRef, profiling
from ray.exceptions import ObjectStoreFullError
from ray._private.client_mode_hook import client_mode_hook

from ray.exceptions import RayError, RayTaskError

logger = logging.getLogger(__name__)


@client_mode_hook
def put(value):
    """Store an object in the object store.

    The object may not be evicted while a reference to the returned ID exists.

    Args:
        value: The Python object to be stored.

    Returns:
        The object ref assigned to this value.
    """
    worker = ray.worker.global_worker
    worker.check_connected()
    with profiling.profile("ray.put"):
        try:
            object_ref = worker.put_object(value, pin_object=True)
        except ObjectStoreFullError:
            logger.info(
                "Put failed since the value was either too large or the "
                "store was full of pinned objects.")
            raise
        return object_ref


# Global variable to make sure we only send out the warning once.
blocking_get_inside_async_warned = False


@client_mode_hook
def get(object_refs, *, timeout=None):
    """Get a remote object or a list of remote objects from the object store.

    This method blocks until the object corresponding to the object ref is
    available in the local object store. If this object is not in the local
    object store, it will be shipped from an object store that has it (once the
    object has been created). If object_refs is a list, then the objects
    corresponding to each object in the list will be returned.

    This method will issue a warning if it's running inside async context,
    you can use ``await object_ref`` instead of ``ray.get(object_ref)``. For
    a list of object refs, you can use ``await asyncio.gather(*object_refs)``.

    Args:
        object_refs: Object ref of the object to get or a list of object refs
            to get.
        timeout (Optional[float]): The maximum amount of time in seconds to
            wait before returning.

    Returns:
        A Python object or a list of Python objects.

    Raises:
        GetTimeoutError: A GetTimeoutError is raised if a timeout is set and
            the get takes longer than timeout to return.
        Exception: An exception is raised if the task that created the object
            or that created one of the objects raised an exception.
    """
    worker = ray.worker.global_worker
    worker.check_connected()

    if hasattr(
            worker,
            "core_worker") and worker.core_worker.current_actor_is_asyncio():
        global blocking_get_inside_async_warned
        if not blocking_get_inside_async_warned:
            logger.warning("Using blocking ray.get inside async actor. "
                           "This blocks the event loop. Please use `await` "
                           "on object ref with asyncio.gather if you want to "
                           "yield execution to the event loop instead.")
            blocking_get_inside_async_warned = True

    with profiling.profile("ray.get"):
        is_individual_id = isinstance(object_refs, ray.ObjectRef)
        if is_individual_id:
            object_refs = [object_refs]

        if not isinstance(object_refs, list):
            raise ValueError("'object_refs' must either be an object ref "
                             "or a list of object refs.")

        global last_task_error_raise_time
        # TODO(ujvl): Consider how to allow user to retrieve the ready objects.
        values, debugger_breakpoint = worker.get_objects(
            object_refs, timeout=timeout)
        for i, value in enumerate(values):
            if isinstance(value, RayError):
                last_task_error_raise_time = time.time()
                if isinstance(value, ray.exceptions.ObjectLostError):
                    worker.core_worker.dump_object_store_memory_usage()
                if isinstance(value, RayTaskError):
                    raise value.as_instanceof_cause()
                else:
                    raise value

        if is_individual_id:
            values = values[0]

        if debugger_breakpoint != b"":
            frame = sys._getframe().f_back
            rdb = ray.util.pdb.connect_ray_pdb(
                None, None, False, None,
                debugger_breakpoint.decode() if debugger_breakpoint else None)
            rdb.set_trace(frame=frame)

        return values


# Global variable to make sure we only send out the warning once.
blocking_wait_inside_async_warned = False


@client_mode_hook
def wait(object_refs, *, num_returns=1, timeout=None, fetch_local=True):
    """Return a list of IDs that are ready and a list of IDs that are not.

    If timeout is set, the function returns either when the requested number of
    IDs are ready or when the timeout is reached, whichever occurs first. If it
    is not set, the function simply waits until that number of objects is ready
    and returns that exact number of object refs.

    This method returns two lists. The first list consists of object refs that
    correspond to objects that are available in the object store. The second
    list corresponds to the rest of the object refs (which may or may not be
    ready).

    Ordering of the input list of object refs is preserved. That is, if A
    precedes B in the input list, and both are in the ready list, then A will
    precede B in the ready list. This also holds true if A and B are both in
    the remaining list.

    This method will issue a warning if it's running inside an async context.
    Instead of ``ray.wait(object_refs)``, you can use
    ``await asyncio.wait(object_refs)``.

    Args:
        object_refs (List[ObjectRef]): List of object refs for objects that may
            or may not be ready. Note that these IDs must be unique.
        num_returns (int): The number of object refs that should be returned.
        timeout (float): The maximum amount of time in seconds to wait before
            returning.
        fetch_local (bool): If True, wait for the object to be downloaded onto
            the local node before returning it as ready. If False, ray.wait()
            will not trigger fetching of objects to the local node and will
            return immediately once the object is available anywhere in the
            cluster.

    Returns:
        A list of object refs that are ready and a list of the remaining object
        IDs.
    """
    worker = ray.worker.global_worker
    worker.check_connected()

    if hasattr(worker,
               "core_worker") and worker.core_worker.current_actor_is_asyncio(
               ) and timeout != 0:
        global blocking_wait_inside_async_warned
        if not blocking_wait_inside_async_warned:
            logger.debug("Using blocking ray.wait inside async method. "
                         "This blocks the event loop. Please use `await` "
                         "on object ref with asyncio.wait. ")
            blocking_wait_inside_async_warned = True

    if isinstance(object_refs, ObjectRef):
        raise TypeError(
            "wait() expected a list of ray.ObjectRef, got a single "
            "ray.ObjectRef")

    if not isinstance(object_refs, list):
        raise TypeError("wait() expected a list of ray.ObjectRef, "
                        f"got {type(object_refs)}")

    if timeout is not None and timeout < 0:
        raise ValueError("The 'timeout' argument must be nonnegative. "
                         f"Received {timeout}")

    for object_ref in object_refs:
        if not isinstance(object_ref, ObjectRef):
            raise TypeError("wait() expected a list of ray.ObjectRef, "
                            f"got list containing {type(object_ref)}")

    # TODO(swang): Check main thread.
    with profiling.profile("ray.wait"):

        # TODO(rkn): This is a temporary workaround for
        # https://github.com/ray-project/ray/issues/997. However, it should be
        # fixed in Arrow instead of here.
        if len(object_refs) == 0:
            return [], []

        if len(object_refs) != len(set(object_refs)):
            raise ValueError("Wait requires a list of unique object refs.")
        if num_returns <= 0:
            raise ValueError(
                "Invalid number of objects to return %d." % num_returns)
        if num_returns > len(object_refs):
            raise ValueError("num_returns cannot be greater than the number "
                             "of objects provided to ray.wait.")

        timeout = timeout if timeout is not None else 10**6
        timeout_milliseconds = int(timeout * 1000)
        ready_ids, remaining_ids = worker.core_worker.wait(
            object_refs,
            num_returns,
            timeout_milliseconds,
            worker.current_task_id,
            fetch_local,
        )
        return ready_ids, remaining_ids

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray
import threading
import numpy as np
import pyarrow.plasma as plasma

PYTHON_MODE = 2


def check_main_thread():
    """Check that we are currently on the main thread.

    Raises:
        Exception: An exception is raised if this is called on a thread other
            than the main thread.
    """
    if threading.current_thread().getName() != "MainThread":
        raise Exception("The Ray methods are not thread safe and must be "
                        "called from the main thread. This method was called "
                        "from thread {}."
                        .format(threading.current_thread().getName()))


def get(object_ids, worker=None):
    """Get a remote object or a collection of remote objects from the object
    store.

    This method blocks until the object corresponding to the object ID is
    available in the local object store. If this object is not in the local
    object store, it will be shipped from an object store that has it (once the
    object has been created). If object_ids is a list, tuple or numpy array,
    then the objects corresponding to each object in the list will be returned.
    If object_ids is a dict, then the objects corresponding to the ObjectIDs in
    the dict's values will be gotten from the object store and the dict will
    return.

    Args:
        object_ids: Object ID of the object to get, a list, tuple, ndarray of
            object IDs to get or a dict of {key: object ID}.

    Returns:
        A Python object, a list of Python objects or a dict of {key: object}.
    """
    from ray.worker import RayTaskError, RayGetError, global_worker

    if worker is None:
        worker = global_worker

    worker.check_connected()
    with ray.log_span("ray:get", worker=worker):
        check_main_thread()

        if worker.mode == PYTHON_MODE:
            # In PYTHON_MODE, ray.get is the identity operation (the input will
            # actually be a value not an objectid).
            return object_ids
        if isinstance(object_ids, (list, tuple, np.ndarray)):
            if isinstance(object_ids, np.ndarray) and object_ids.ndim > 1:
                raise TypeError("get() expected a 1D array of ObjectIDs")
            values = worker.get_object(object_ids)
            for i, value in enumerate(values):
                if isinstance(value, RayTaskError):
                    raise RayGetError(object_ids[i], value)
            return values
        elif isinstance(object_ids, dict):
            to_get = [
                t for t in object_ids.items()
                if isinstance(t[1], ray.ObjectID)
            ]
            keys, oids = [list(t) for t in zip(*to_get)]
            values = worker.get_object(oids)
            for i, value in enumerate(values):
                if isinstance(value, RayTaskError):
                    raise RayGetError(object_ids[i], value)
            result = object_ids.copy()
            for key, val in zip(keys, values):
                result[key] = val
            return result
        else:
            value = worker.get_object([object_ids])[0]
            if isinstance(value, RayTaskError):
                # If the result is a RayTaskError, then the task that created
                # this object failed, and we should propagate the error message
                # here.
                raise RayGetError(object_ids, value)
            return value


def wait(object_ids, num_returns=1, timeout=None, worker=None):
    """Return a list of IDs that are ready and a list of IDs that are not.

    If timeout is set, the function returns either when the requested number of
    IDs are ready or when the timeout is reached, whichever occurs first. If it
    is not set, the function simply waits until that number of objects is ready
    and returns that exact number of object_ids.

    This method returns two lists. The first list consists of object IDs that
    correspond to objects that are stored in the object store. The second list
    corresponds to the rest of the object IDs (which may or may not be ready).

    Args:
        object_ids (List[ObjectID], Tuple(ObjectID), np.array(ObjectID)):
            List like of object IDs for objects that may or may not be ready.
            Note that these IDs must be unique.
        num_returns (int): The number of object IDs that should be returned.
        timeout (int): The maximum amount of time in milliseconds to wait
            before returning.

    Returns:
        A list of object IDs that are ready and a list of the remaining object
            IDs.
    """
    from ray.worker import global_worker

    if worker is None:
        worker = global_worker

    if worker.use_raylet:
        print("plasma_client.wait has not been implemented yet")
        return

    if isinstance(object_ids, ray.ObjectID):
        raise TypeError(
            "wait() expected a list like of ObjectIDs, got a single ObjectID")

    if not isinstance(object_ids, (list, tuple, np.ndarray)):
        raise TypeError("wait() expected a list like of ObjectIDs, got {}"
                        .format(type(object_ids)))

    if isinstance(object_ids, np.ndarray) and object_ids.ndim > 1:
        raise TypeError("wait() expected a 1D array of ObjectIDs")

    if worker.mode != PYTHON_MODE:
        for object_id in object_ids:
            if not isinstance(object_id, ray.ObjectID):
                raise TypeError("wait() expected a list like of ObjectIDs, "
                                "got list containing {}".format(
                                    type(object_id)))

    worker.check_connected()
    with ray.log_span("ray:wait", worker=worker):
        check_main_thread()

        # When Ray is run in PYTHON_MODE, all functions are run immediately,
        # so all objects in object_id are ready.
        if worker.mode == PYTHON_MODE:
            return object_ids[:num_returns], object_ids[num_returns:]

        # TODO(rkn): This is a temporary workaround for
        # https://github.com/ray-project/ray/issues/997. However, it should be
        # fixed in Arrow instead of here.
        if len(object_ids) == 0:
            return [], []

        object_id_strs = [
            plasma.ObjectID(object_id.id()) for object_id in object_ids
        ]
        timeout = timeout if timeout is not None else 2**30
        ready_ids, remaining_ids = worker.plasma_client.wait(
            object_id_strs, timeout, num_returns)
        ready_ids = [
            ray.ObjectID(object_id.binary()) for object_id in ready_ids
        ]
        remaining_ids = [
            ray.ObjectID(object_id.binary()) for object_id in remaining_ids
        ]
        return ready_ids, remaining_ids

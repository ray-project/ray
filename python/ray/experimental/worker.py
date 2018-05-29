from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray
import numpy as np


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
    if isinstance(object_ids, (list, tuple, np.ndarray)):
        # There is a dependency on ray.worker which prevents importing
        # global_worker at the top of this file
        return ray.get(list(object_ids)) if worker is None else \
            ray.get(list(object_ids), worker)
    elif isinstance(object_ids, dict):
        to_get = [
            t for t in object_ids.items()
            if isinstance(t[1], ray.ObjectID)
        ]
        keys, oids = [list(t) for t in zip(*to_get)]
        values = ray.get(oids) if worker is None else ray.get(oids, worker)
        result = object_ids.copy()
        for key, val in zip(keys, values):
            result[key] = val
        return result
    else:
        return ray.get(object_ids) if worker is None else \
            ray.get(object_ids, worker)


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
    if isinstance(object_ids, (list, tuple, np.ndarray)):
        return ray.wait(list(object_ids)) if worker is None else \
            ray.wait(list(object_ids), worker)

    return ray.wait(object_ids) if worker is None else \
        ray.wait(object_ids, worker)

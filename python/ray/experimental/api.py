from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray
import numpy as np


def get(object_ids, worker=None):
    """Get a single or a collection of remote objects from the object store.

    This method is identical to `ray.get` except it adds support for tuples,
    ndarrays and dictionaries.

    Args:
        object_ids: Object ID of the object to get, a list, tuple, ndarray of
            object IDs to get or a dict of {key: object ID}.

    Returns:
        A Python object, a list of Python objects or a dict of {key: object}.
    """
    # There is a dependency on ray.worker which prevents importing
    # global_worker at the top of this file
    worker = ray.worker.global_worker if worker is None else worker
    if isinstance(object_ids, (tuple, np.ndarray)):
        return ray.get(list(object_ids), worker)
    elif isinstance(object_ids, dict):
        keys_to_get = [
            k for k, v in object_ids.items() if isinstance(v, ray.ObjectID)
        ]
        ids_to_get = [
            v for k, v in object_ids.items() if isinstance(v, ray.ObjectID)
        ]
        values = ray.get(ids_to_get)

        result = object_ids.copy()
        for key, value in zip(keys_to_get, values):
            result[key] = value
        return result
    else:
        return ray.get(object_ids, worker)


def wait(object_ids, num_returns=1, timeout=None, worker=None):
    """Return a list of IDs that are ready and a list of IDs that are not.

    This method is identical to `ray.wait` except it adds support for tuples
    and ndarrays.

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
    worker = ray.worker.global_worker if worker is None else worker
    if isinstance(object_ids, (tuple, np.ndarray)):
        return ray.wait(list(object_ids), num_returns, timeout, worker)

    return ray.wait(object_ids, num_returns, timeout, worker)

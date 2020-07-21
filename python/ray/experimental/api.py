import ray
import numpy as np


def get(object_refs):
    """Get a single or a collection of remote objects from the object store.

    This method is identical to `ray.get` except it adds support for tuples,
    ndarrays and dictionaries.

    Args:
        object_refs: Object ref of the object to get, a list, tuple, ndarray of
            object refs to get or a dict of {key: object ref}.

    Returns:
        A Python object, a list of Python objects or a dict of {key: object}.
    """
    if isinstance(object_refs, (tuple, np.ndarray)):
        return ray.get(list(object_refs))
    elif isinstance(object_refs, dict):
        keys_to_get = [
            k for k, v in object_refs.items() if isinstance(v, ray.ObjectRef)
        ]
        ids_to_get = [
            v for k, v in object_refs.items() if isinstance(v, ray.ObjectRef)
        ]
        values = ray.get(ids_to_get)

        result = object_refs.copy()
        for key, value in zip(keys_to_get, values):
            result[key] = value
        return result
    else:
        return ray.get(object_refs)


def wait(object_refs, num_returns=1, timeout=None):
    """Return a list of IDs that are ready and a list of IDs that are not.

    This method is identical to `ray.wait` except it adds support for tuples
    and ndarrays.

    Args:
        object_refs (List[ObjectRef], Tuple(ObjectRef), np.array(ObjectRef)):
            List like of object refs for objects that may or may not be ready.
            Note that these IDs must be unique.
        num_returns (int): The number of object refs that should be returned.
        timeout (float): The maximum amount of time in seconds to wait before
            returning.

    Returns:
        A list of object refs that are ready and a list of the remaining object
            IDs.
    """
    if isinstance(object_refs, (tuple, np.ndarray)):
        return ray.wait(
            list(object_refs), num_returns=num_returns, timeout=timeout)

    return ray.wait(object_refs, num_returns=num_returns, timeout=timeout)

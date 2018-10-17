from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray.local_scheduler
import ray.worker
from ray import profiling

__all__ = ["free"]


def free(object_ids, local_only=False, worker=None):
    """Free a list of IDs from object stores.

    This function is a low-level API which should be used in restricted
    scenarios.

    If local_only is false, the request will be send to all object stores.

    This method will not return any value to indicate whether the deletion is
    successful or not. This function is an instruction to object store. If
    the some of the objects are in use, object stores will delete them later
    when the ref count is down to 0.

    Args:
        object_ids (List[ObjectID]): List of object IDs to delete.
        local_only (bool): Whether only deleting the list of objects in local
            object store or all object stores.
    """
    if worker is None:
        worker = ray.worker.get_global_worker()

    if isinstance(object_ids, ray.ObjectID):
        object_ids = [object_ids]

    if not isinstance(object_ids, list):
        raise TypeError("free() expects a list of ObjectID, got {}".format(
            type(object_ids)))

    worker.check_connected()
    with profiling.profile("ray.free", worker=worker):
        if len(object_ids) == 0:
            return

        if worker.use_raylet:
            worker.local_scheduler_client.free(object_ids, local_only)
        else:
            raise Exception("Free is not supported in legacy backend.")

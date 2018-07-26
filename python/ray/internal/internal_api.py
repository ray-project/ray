from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray.local_scheduler
import ray.worker
from ray import profiling
import pyarrow.plasma as plasma

__all__ = ["free"]


def free(object_ids, spread=True, worker=ray.worker.get_global_worker()):
    """Free a list of IDs from object stores.

    This function is low-level API which should be used in restricted
    scenarios.

    If spread is set, the request will spread to all object stores.

    This method will not return any value to indicate whether the deletion is
    successful or not. This function is an instruction to object store. If
    the some of the objects are in use, object stores will delete them later
    when the ref count is down to 0.

    Args:
        object_ids (List[ObjectID]): List of object IDs to delete.
        spread (bool): Whether deleting the list of objects in all object
                       stores.

    """

    if isinstance(object_ids, ray.ObjectID):
        raise TypeError(
            "free() expected a list of ObjectID, got a single ObjectID")

    if not isinstance(object_ids, list):
        raise TypeError("free() expected a list of ObjectID, got {}".format(
            type(object_ids)))
    worker.check_connected()
    with profiling.profile("ray.free", worker=worker):

        if len(object_ids) == 0:
            return

        if len(object_ids) != len(set(object_ids)):
            raise Exception("Free requires a list of unique object IDs.")
        if worker.use_raylet:
            plain_object_ids = [
                plasma.ObjectID(object_id.id()) for object_id in object_ids
            ]
            worker.local_scheduler_client.free(object_ids, spread)
        else:
            raise Exception("Free is not supported in legacy backend.")

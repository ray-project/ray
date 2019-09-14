from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray.worker
from ray import profiling

__all__ = ["free", "pin_object_data"]


def pin_object_data(object_id):
    """Pin the object data referenced by this object id in memory.

    The object data cannot be evicted while there exists a Python reference to
    the object id passed to this function. In order to pin the object, we will
    also download the object to the current node (this overhead is unavoidable
    for now without a distributed ref counting solution).

    Examples:
        >>> x_id = f.remote()
        >>> x_id = pin_object_id(x_id)  # x pinned, cannot be evicted
        >>> del x_id  # x can be evicted again

    Note that ray will automatically do this for objects created with
    ray.put() already, unless you ray.put with weakref=True.
    """
    worker = ray.worker.get_global_worker()

    object_id.set_buffer_ref(
        worker.core_worker.get_objects([object_id], worker.current_task_id))


def unpin_object_data(object_id):
    """Unpin an object pinned by pin_object_id.

    Examples:
        >>> x_id = f.remote()
        >>> pin_object_id(x_id)
        >>> unpin_object_id(x_id)  # as if the pin didn't happen
    """

    object_id.set_buffer_ref(None)


def free(object_ids, local_only=False, delete_creating_tasks=False):
    """Free a list of IDs from object stores.

    This function is a low-level API which should be used in restricted
    scenarios.

    If local_only is false, the request will be send to all object stores.

    This method will not return any value to indicate whether the deletion is
    successful or not. This function is an instruction to object store. If
    the some of the objects are in use, object stores will delete them later
    when the ref count is down to 0.

    Examples:
        >>> x_id = f.remote()
        >>> ray.get(x_id)  # wait for x to be created first
        >>> free([x_id])  # unpin & delete x globally

    Args:
        object_ids (List[ObjectID]): List of object IDs to delete.
        local_only (bool): Whether only deleting the list of objects in local
            object store or all object stores.
        delete_creating_tasks (bool): Whether also delete the object creating
            tasks.
    """
    worker = ray.worker.get_global_worker()

    if isinstance(object_ids, ray.ObjectID):
        object_ids = [object_ids]

    if not isinstance(object_ids, list):
        raise TypeError("free() expects a list of ObjectID, got {}".format(
            type(object_ids)))

    # Make sure that the values are object IDs.
    for object_id in object_ids:
        if not isinstance(object_id, ray.ObjectID):
            raise TypeError("Attempting to call `free` on the value {}, "
                            "which is not an ray.ObjectID.".format(object_id))
        unpin_object_data(object_id)

    if ray.worker._mode() == ray.worker.LOCAL_MODE:
        worker.local_mode_manager.free(object_ids)
        return

    worker.check_connected()
    with profiling.profile("ray.free"):
        if len(object_ids) == 0:
            return

        worker.core_worker.free_objects(object_ids, local_only,
                                        delete_creating_tasks)

import ray


def force_spill_objects(object_refs):
    """ Force spilling objects to external storage.
    """
    core_worker = ray.worker.global_worker.core_worker
    return core_worker.force_spill_objects(object_refs)


def force_restore_spilled_objects(object_refs):
    """ Force restoring objects from external storage.
    """
    core_worker = ray.worker.global_worker.core_worker
    return core_worker.force_restore_spilled_objects(object_refs)

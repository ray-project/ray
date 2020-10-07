import ray


def force_spill_objects(object_refs):
    """Force spilling objects to external storage.

    Args:
        object_refs: Object refs of the objects to be
            spilled.
    """
    core_worker = ray.worker.global_worker.core_worker
    # Make sure that the values are object refs.
    for object_ref in object_refs:
        if not isinstance(object_ref, ray.ObjectRef):
            raise TypeError(
                f"Attempting to call `force_spill_objects` on the "
                f"value {object_ref}, which is not an ray.ObjectRef.")
    return core_worker.force_spill_objects(object_refs)

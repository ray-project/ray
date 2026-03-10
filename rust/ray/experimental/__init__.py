# Ray experimental package (Rust backend)


def set_target_for_ref(ref, tensor_list):
    """Associate pre-allocated tensor buffers with an ObjectRef.

    When ray.get is called on this ref, the received tensors will be
    written into the provided buffers instead of allocating new ones.
    """
    ref._target_tensors = tensor_list


def wait_tensor_freed(tensor, timeout=None):
    """Wait until a tensor is freed from the RDT object store.

    The tensor is freed when all ObjectRefs containing it go out of scope.
    """
    from ray._private.worker import global_worker

    if global_worker.rdt_manager is None:
        raise RuntimeError("RDT manager not initialized")
    global_worker.rdt_manager.rdt_store.wait_tensor_freed(tensor, timeout=timeout)

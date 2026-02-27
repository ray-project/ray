import ray._private.worker


def get_reference_counter_debug_json() -> str:
    """Returns the full state of the reference counter as a JSON string.

    This is NOT a stable API. It should only be used for debugging and
    NEVER in tests or production code.
    """
    worker = ray._private.worker.global_worker
    worker.check_connected()
    return worker.core_worker.get_reference_counter_debug_json()

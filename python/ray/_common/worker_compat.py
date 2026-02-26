"""Worker compatibility layer for libraries (e.g. Serve).

Allows setting worker-related flags without importing ray._private.worker directly.
"""


def set_blocking_get_inside_async_warned(value: bool) -> None:
    """Set the flag that mutes 'blocking ray.get inside async' warnings."""
    from ray._private import worker

    worker.blocking_get_inside_async_warned = value

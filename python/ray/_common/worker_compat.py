"""Worker compatibility layer for libraries (e.g. Serve).

Allows libraries to use worker/node state via ``ray._common`` instead of importing
``ray._private.worker`` directly.
"""

from typing import Any, Optional, Tuple

from ray._private import worker


def set_blocking_get_inside_async_warned(value: bool) -> None:
    """Set the flag that mutes 'blocking ray.get inside async' warnings."""
    worker.blocking_get_inside_async_warned = value


def set_filter_logs_by_job(value: bool) -> None:
    """Control whether worker log lines are filtered by the current job."""
    worker.global_worker._filter_logs_by_job = value


def get_serialization_context() -> Any:
    """Return the global worker serialization context."""
    return worker.global_worker.get_serialization_context()


def has_global_node() -> bool:
    """Return whether the process currently has an initialized global node."""
    return worker._global_node is not None


def get_global_node_logs_dir_path() -> Optional[str]:
    """Return global node logs dir path, if available."""
    if worker._global_node is None:
        return None
    return worker._global_node.get_logs_dir_path()


def get_global_node_log_rotation_config() -> Tuple[int, int]:
    """Return ``(max_bytes, backup_count)`` from the global node."""
    return worker._global_node.max_bytes, worker._global_node.backup_count


def get_current_session_name() -> Optional[str]:
    """Return the current global worker session name, if available."""
    node = getattr(worker.global_worker, "node", None)
    if node is None:
        return None
    return node.session_name

import concurrent.futures
import threading
from typing import Optional

from ray.serve._private.constants import RAY_SERVE_REPLICA_HANDLE_RESOLVER_THREADS

_replica_handle_resolver: Optional[concurrent.futures.ThreadPoolExecutor] = None
_replica_handle_resolver_lock = threading.Lock()


def get_replica_handle_resolver() -> concurrent.futures.ThreadPoolExecutor:
    """Return the process-wide executor for blocking actor handle lookups."""
    global _replica_handle_resolver
    if _replica_handle_resolver is None:
        with _replica_handle_resolver_lock:
            if _replica_handle_resolver is None:
                _replica_handle_resolver = concurrent.futures.ThreadPoolExecutor(
                    max_workers=RAY_SERVE_REPLICA_HANDLE_RESOLVER_THREADS,
                    thread_name_prefix="serve-replica-handle-resolver",
                )

    return _replica_handle_resolver

from concurrent.futures import ThreadPoolExecutor

from ray.serve._private.constants import RAY_SERVE_REPLICA_HANDLE_RESOLVER_THREADS

# Keep the unpicklable executor outside the request_router package, which
# RequestRouterConfig registers for serialization by value for the default router.
_executor = ThreadPoolExecutor(
    max_workers=RAY_SERVE_REPLICA_HANDLE_RESOLVER_THREADS,
    thread_name_prefix="serve-replica-handle",
)


def get_replica_handle_executor() -> ThreadPoolExecutor:
    return _executor

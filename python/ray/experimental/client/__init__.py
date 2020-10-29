import ray
from ray.experimental.client.worker import Worker
from typing import Optional

_client_worker: Optional[Worker] = None
_in_cluster: bool = True


def _fallback_to_cluster(in_cluster, client_worker_method):
    def fallback_func(*args, **kwargs):
        global _in_cluster
        global _client_worker
        if _in_cluster:
            in_cluster(*args, **kwargs)
        else:
            _client_worker.__getattr__(client_worker_method)(*args, **kwargs)
    return fallback_func


get = _fallback_to_cluster(ray.get, 'get')

put = _fallback_to_cluster(ray.put, 'put')

remote = _fallback_to_cluster(ray.remote, 'remote')

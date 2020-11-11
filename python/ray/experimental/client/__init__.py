import ray
from ray.experimental.client.worker import Worker
from typing import Optional

_client_worker: Optional[Worker] = None
_in_cluster: bool = True


def get(*args, **kwargs):
    global _client_worker
    global _in_cluster
    if _in_cluster:
        return ray.get(*args, **kwargs)
    else:
        return _client_worker.get(*args, **kwargs)


def put(*args, **kwargs):
    global _client_worker
    global _in_cluster
    if _in_cluster:
        return ray.put(*args, **kwargs)
    else:
        return _client_worker.put(*args, **kwargs)


def remote(*args, **kwargs):
    pass


def connect(conn_str):
    global _in_cluster
    global _client_worker
    _in_cluster = False
    _client_worker = Worker(conn_str)


def disconnect():
    global _in_cluster
    global _client_worker
    if _client_worker is not None:
        _client_worker.close()
    _in_cluster = True
    _client_worker = None

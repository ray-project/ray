from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray

_local = {}  # dict for local mode


def _internal_kv_initialized():
    worker = ray.worker.get_global_worker()
    return hasattr(worker, "mode") and worker.mode is not None


def _internal_kv_get(key):
    """Fetch the value of a binary key."""

    worker = ray.worker.get_global_worker()
    if worker.mode == ray.worker.LOCAL_MODE:
        return _local.get(key)

    return worker.redis_client.hget(key, "value")


def _internal_kv_put(key, value, overwrite=False):
    """Globally associates a value with a given binary key.

    This only has an effect if the key does not already have a value.

    Returns:
        already_exists (bool): whether the value already exists.
    """

    worker = ray.worker.get_global_worker()
    if worker.mode == ray.worker.LOCAL_MODE:
        exists = key in _local
        if not exists or overwrite:
            _local[key] = value
        return exists

    if overwrite:
        updated = worker.redis_client.hset(key, "value", value)
    else:
        updated = worker.redis_client.hsetnx(key, "value", value)
    return updated == 0  # already exists

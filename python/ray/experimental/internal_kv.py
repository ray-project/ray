from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray


def __internal_kv_initialized():
    worker = ray.worker.get_global_worker()
    return hasattr(worker, "worker_id")


def __internal_kv_get(key):
    """Fetch the value of a binary key."""

    worker = ray.worker.get_global_worker()
    return worker.redis_client.hget(key, "value")


def __internal_kv_put(key, value):
    """Globally associates a value with a given binary key.

    This only has an effect if the key does not already have a value.

    Returns
        already_exists (bool): whether the value already exists.
    """

    updated = worker.redis_client.hsetnx(actor_hash, name, pickled_state)
    return updated == 0  # already exists

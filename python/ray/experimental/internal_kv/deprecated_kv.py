from typing import List, Union

import ray
import os
from ray._private.client_mode_hook import client_mode_hook
from collections import namedtuple
from ray._private.services import create_redis_client
import redis
redis_client = None

Target = namedtuple("Target", ["ip", "port", "password"])

@client_mode_hook
def _internal_kv_initialized(target = None):
    global redis_client
    if redis_client is not None:
        return True
    if target is None:
        worker = ray.worker.global_worker
        if hasattr(worker, "mode") and worker.mode is not None:
            redis_client = ray.worker.global_worker.redis_client
        print(redis_client)
    elif isinstance(target, redis.Redis):
        redis_client = target
    else:
        redis_client = create_redis_client(target.ip + ":" + str(target.port), target.password)
    return redis_client is not None


@client_mode_hook
def _internal_kv_get(key: Union[str, bytes]) -> bytes:
    """Fetch the value of a binary key."""

    return redis_client.hget(key, "value")


@client_mode_hook
def _internal_kv_exists(key: Union[str, bytes]) -> bool:
    """Check key exists or not."""
    return redis_client.hexists(key, "value")


@client_mode_hook
def _internal_kv_put(key: Union[str, bytes],
                     value: Union[str, bytes],
                     overwrite: bool = True) -> bool:
    """Globally associates a value with a given binary key.

    This only has an effect if the key does not already have a value.

    Returns:
        already_exists (bool): whether the value already exists.
    """

    worker = ray.worker.global_worker

    if overwrite:
        updated = worker.redis_client.hset(key, "value", value)
    else:
        updated = worker.redis_client.hsetnx(key, "value", value)
    return updated == 0  # already exists


@client_mode_hook
def _internal_kv_del(key: Union[str, bytes]):
    return redis_client.delete(key)


@client_mode_hook
def _internal_kv_list(prefix: Union[str, bytes]) -> List[bytes]:
    """List all keys in the internal KV store that start with the prefix."""
    if isinstance(prefix, bytes):
        pattern = prefix + b"*"
    else:
        pattern = prefix + "*"
    return redis_client.keys(pattern=pattern)

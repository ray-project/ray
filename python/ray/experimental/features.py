from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray

OBJECT_INFO_PREFIX = b"OI:"
OBJECT_LOCATION_PREFIX = b"OL:"
TASK_TABLE_PREFIX = b"TT:"


def flush_redis_unsafe():
    """This removes some non-critical state from the primary Redis shard.

    This removes the log files as well as the event log from Redis. This can
    be used to try to address out-of-memory errors caused by the accumulation
    of metadata in Redis. However, it will only partially address the issue as
    much of the data is in the task table (and object table), which are not
    flushed.
    """
    if not hasattr(ray.worker.global_worker, "redis_client"):
        raise Exception("ray.experimental.flush_redis_unsafe cannot be called "
                        "before ray.init() has been called.")

    redis_client = ray.worker.global_worker.redis_client

    # Delete the log files from the primary Redis shard.
    keys = redis_client.keys("LOGFILE:*")
    if len(keys) > 0:
        num_deleted = redis_client.delete(*keys)
    else:
        num_deleted = 0
    print("Deleted {} log files from Redis.".format(num_deleted))

    # Delete the event log from the primary Redis shard.
    keys = redis_client.keys("event_log:*")
    if len(keys) > 0:
        num_deleted = redis_client.delete(*keys)
    else:
        num_deleted = 0
    print("Deleted {} event logs from Redis.".format(num_deleted))


def flush_task_and_object_metadata_unsafe():
    """This removes some critical state from the Redis shards.

    In a multitenant environment, this will flush metadata for all jobs, which
    may be undesirable.

    This removes all of the object and task metadata. This can be used to try
    to address out-of-memory errors caused by the accumulation of metadata in
    Redis. However, after running this command, fault tolerance will most
    likely not work.
    """
    if not hasattr(ray.worker.global_worker, "redis_client"):
        raise Exception("ray.experimental.flush_redis_unsafe cannot be called "
                        "before ray.init() has been called.")

    def flush_shard(redis_client):
        # Flush the task table. Note that this also flushes the driver tasks
        # which may be undesirable.
        num_task_keys_deleted = 0
        for key in redis_client.scan_iter(match=TASK_TABLE_PREFIX + b"*"):
            num_task_keys_deleted += redis_client.delete(key)
        print("Deleted {} task keys from Redis.".format(num_task_keys_deleted))

        # Flush the object information.
        num_object_keys_deleted = 0
        for key in redis_client.scan_iter(match=OBJECT_INFO_PREFIX + b"*"):
            num_object_keys_deleted += redis_client.delete(key)
        print("Deleted {} object info keys from Redis.".format(
            num_object_keys_deleted))

        # Flush the object locations.
        num_object_location_keys_deleted = 0
        for key in redis_client.scan_iter(match=OBJECT_LOCATION_PREFIX + b"*"):
            num_object_location_keys_deleted += redis_client.delete(key)
        print("Deleted {} object location keys from Redis.".format(
            num_object_location_keys_deleted))

    # Loop over the shards and flush all of them.
    for redis_client in ray.worker.global_state.redis_clients:
        flush_shard(redis_client)

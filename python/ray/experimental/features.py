from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray
from ray.utils import binary_to_hex

OBJECT_INFO_PREFIX = b"OI:"
OBJECT_LOCATION_PREFIX = b"OL:"
TASK_PREFIX = b"TT:"


def flush_redis_unsafe(redis_client=None):
    """This removes some non-critical state from the primary Redis shard.

    This removes the log files as well as the event log from Redis. This can
    be used to try to address out-of-memory errors caused by the accumulation
    of metadata in Redis. However, it will only partially address the issue as
    much of the data is in the task table (and object table), which are not
    flushed.

    Args:
      redis_client: optional, if not provided then ray.init() must have been
        called.
    """
    if redis_client is None:
        ray.worker.global_worker.check_connected()
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
    ray.worker.global_worker.check_connected()

    def flush_shard(redis_client):
        # Flush the task table. Note that this also flushes the driver tasks
        # which may be undesirable.
        num_task_keys_deleted = 0
        for key in redis_client.scan_iter(match=TASK_PREFIX + b"*"):
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


def _task_table_shard(shard_index):
    redis_client = ray.global_state.redis_clients[shard_index]
    task_table_keys = redis_client.keys(TASK_PREFIX + b"*")
    results = {}
    for key in task_table_keys:
        task_id_binary = key[len(TASK_PREFIX):]
        results[binary_to_hex(task_id_binary)] = ray.global_state._task_table(
            ray.TaskID(task_id_binary))

    return results


def _object_table_shard(shard_index):
    redis_client = ray.global_state.redis_clients[shard_index]
    object_table_keys = redis_client.keys(OBJECT_LOCATION_PREFIX + b"*")
    results = {}
    for key in object_table_keys:
        object_id_binary = key[len(OBJECT_LOCATION_PREFIX):]
        results[binary_to_hex(object_id_binary)] = (
            ray.global_state._object_table(ray.ObjectID(object_id_binary)))

    return results


def _flush_finished_tasks_unsafe_shard(shard_index):
    ray.worker.global_worker.check_connected()

    redis_client = ray.global_state.redis_clients[shard_index]
    tasks = _task_table_shard(shard_index)

    keys_to_delete = []
    for task_id, task_info in tasks.items():
        if task_info["State"] == ray.experimental.state.TASK_STATUS_DONE:
            keys_to_delete.append(TASK_PREFIX +
                                  ray.utils.hex_to_binary(task_id))

    num_task_keys_deleted = 0
    if len(keys_to_delete) > 0:
        num_task_keys_deleted = redis_client.execute_command(
            "del", *keys_to_delete)

    print("Deleted {} finished tasks from Redis shard."
          .format(num_task_keys_deleted))


def _flush_evicted_objects_unsafe_shard(shard_index):
    ray.worker.global_worker.check_connected()

    redis_client = ray.global_state.redis_clients[shard_index]
    objects = _object_table_shard(shard_index)

    keys_to_delete = []
    for object_id, object_info in objects.items():
        if object_info["ManagerIDs"] == []:
            keys_to_delete.append(OBJECT_LOCATION_PREFIX +
                                  ray.utils.hex_to_binary(object_id))
            keys_to_delete.append(OBJECT_INFO_PREFIX +
                                  ray.utils.hex_to_binary(object_id))

    num_object_keys_deleted = 0
    if len(keys_to_delete) > 0:
        num_object_keys_deleted = redis_client.execute_command(
            "del", *keys_to_delete)

    print("Deleted {} keys for evicted objects from Redis."
          .format(num_object_keys_deleted))


def flush_finished_tasks_unsafe():
    """This removes some critical state from the Redis shards.

    In a multitenant environment, this will flush metadata for all jobs, which
    may be undesirable.

    This removes all of the metadata for finished tasks. This can be used to
    try to address out-of-memory errors caused by the accumulation of metadata
    in Redis. However, after running this command, fault tolerance will most
    likely not work.
    """
    ray.worker.global_worker.check_connected()

    for shard_index in range(len(ray.global_state.redis_clients)):
        _flush_finished_tasks_unsafe_shard(shard_index)


def flush_evicted_objects_unsafe():
    """This removes some critical state from the Redis shards.

    In a multitenant environment, this will flush metadata for all jobs, which
    may be undesirable.

    This removes all of the metadata for objects that have been evicted. This
    can be used to try to address out-of-memory errors caused by the
    accumulation of metadata in Redis. However, after running this command,
    fault tolerance will most likely not work.
    """
    ray.worker.global_worker.check_connected()

    for shard_index in range(len(ray.global_state.redis_clients)):
        _flush_evicted_objects_unsafe_shard(shard_index)

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray


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

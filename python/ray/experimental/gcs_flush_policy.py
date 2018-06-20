from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import time

import ray
import ray.cloudpickle as pickle


class GcsFlushPolicy(object):
    """Experimental: a policy to control GCS flushing.

    Used by Monitor to enable automatic control of memory usage.
    """

    def should_flush(self, redis_client):
        """Returns a bool, whether a flush request should be issued."""
        pass

    def num_entries_to_flush(self):
        """Returns an upper bound for number of entries to flush next."""
        pass

    def record_flush(self):
        """Must be called after a flush has been performed."""
        pass


class SimpleGcsFlushPolicy(GcsFlushPolicy):
    """A simple policy with constant flush rate, after a warmup period.

    Example policy values:
        flush_when_at_least_bytes 2GB
        flush_period_secs 10s
        flush_num_entries_each_time 10k

    This means
    (1) If the GCS shard uses less than 2GB of memory, no flushing would take
        place.  This should cover most Ray runs.
    (2) The GCS shard will only honor a flush request, if it's issued after 10
        seconds since the last processed flush.  In particular this means it's
        okay for the Monitor to issue requests more frequently than this param.
    (3) When processing a flush, the shard will flush at most 10k entries.
        This is to control the latency of each request.

    Note, flush rate == (flush period) * (num entries each time).  So
    applications that have a heavier GCS load can tune these params.
    """

    def __init__(self,
                 flush_when_at_least_bytes=(1 << 31),
                 flush_period_secs=10,
                 flush_num_entries_each_time=10000):
        self.flush_when_at_least_bytes = flush_when_at_least_bytes
        self.flush_period_secs = flush_period_secs
        self.flush_num_entries_each_time = flush_num_entries_each_time
        self.last_flush_timestamp = time.time()

    def should_flush(self, redis_client):
        if time.time() - self.last_flush_timestamp < self.flush_period_secs:
            return False

        used_memory = redis_client.info("memory")["used_memory"]
        assert used_memory > 0

        return used_memory >= self.flush_when_at_least_bytes

    def num_entries_to_flush(self):
        return self.flush_num_entries_each_time

    def record_flush(self):
        self.last_flush_timestamp = time.time()

    def serialize(self):
        return pickle.dumps(self)


def set_flushing_policy(flushing_policy):
    """Serialize this policy for Monitor to pick up."""
    if "RAY_USE_NEW_GCS" not in os.environ:
        raise Exception(
            "set_flushing_policy() is only available when environment "
            "variable RAY_USE_NEW_GCS is present at both compile and run time."
        )
    ray.worker.global_worker.check_connected()
    redis_client = ray.worker.global_worker.redis_client

    serialized = pickle.dumps(flushing_policy)
    redis_client.set("gcs_flushing_policy", serialized)

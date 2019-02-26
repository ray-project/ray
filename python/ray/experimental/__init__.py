from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from .features import (
    flush_redis_unsafe, flush_task_and_object_metadata_unsafe,
    flush_finished_tasks_unsafe, flush_evicted_objects_unsafe,
    _flush_finished_tasks_unsafe_shard, _flush_evicted_objects_unsafe_shard)
from .gcs_flush_policy import (set_flushing_policy, GcsFlushPolicy,
                               SimpleGcsFlushPolicy)
from .named_actors import get_actor, register_actor
from .api import get, wait


def TensorFlowVariables(*args, **kwargs):
    raise DeprecationWarning(
        "'ray.experimental.TensorFlowVariables' is deprecated. Instead, please"
        " do 'from ray.experimental.tf_utils import TensorFlowVariables'.")


__all__ = [
    "TensorFlowVariables", "flush_redis_unsafe",
    "flush_task_and_object_metadata_unsafe", "flush_finished_tasks_unsafe",
    "flush_evicted_objects_unsafe", "_flush_finished_tasks_unsafe_shard",
    "_flush_evicted_objects_unsafe_shard", "get_actor", "register_actor",
    "get", "wait", "set_flushing_policy", "GcsFlushPolicy",
    "SimpleGcsFlushPolicy"
]

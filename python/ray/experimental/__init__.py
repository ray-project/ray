from .gcs_flush_policy import (set_flushing_policy, GcsFlushPolicy,
                               SimpleGcsFlushPolicy)
from .named_actors import get_actor, register_actor
from .api import get, wait
from .actor_pool import ActorPool
from .dynamic_resources import set_resource
from . import iter


def TensorFlowVariables(*args, **kwargs):
    raise DeprecationWarning(
        "'ray.experimental.TensorFlowVariables' is deprecated. Instead, please"
        " do 'from ray.experimental.tf_utils import TensorFlowVariables'.")


__all__ = [
    "TensorFlowVariables",
    "get_actor",
    "register_actor",
    "get",
    "wait",
    "set_flushing_policy",
    "GcsFlushPolicy",
    "SimpleGcsFlushPolicy",
    "set_resource",
    "ActorPool",
    "iter",
]

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from .gcs_flush_policy import (set_flushing_policy, GcsFlushPolicy,
                               SimpleGcsFlushPolicy)
from .named_actors import get_actor, register_actor
from .api import get, wait
from .dynamic_resources import set_resource


def TensorFlowVariables(*args, **kwargs):
    raise DeprecationWarning(
        "'ray.experimental.TensorFlowVariables' is deprecated. Instead, please"
        " do 'from ray.experimental.tf_utils import TensorFlowVariables'.")


__all__ = [
    "TensorFlowVariables", "get_actor", "register_actor", "get", "wait",
    "set_flushing_policy", "GcsFlushPolicy", "SimpleGcsFlushPolicy",
    "set_resource"
]

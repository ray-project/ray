import warnings
from ray.util.annotations import Deprecated

from ray.train.hf_transformers.transformers_checkpoint import (
    TransformersCheckpoint,
)

from ._deprecation_msg import deprecation_msg


@Deprecated(message=deprecation_msg)
class HuggingFaceCheckpoint(TransformersCheckpoint):
    # Use __new__ as it is much less likely to be overriden
    # than __init__
    def __new__(cls: type, *args, **kwargs):
        warnings.warn(deprecation_msg, DeprecationWarning)
        return super(HuggingFaceCheckpoint, cls).__new__(cls, *args, **kwargs)


__all__ = [
    "HuggingFaceCheckpoint",
]

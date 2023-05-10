import warnings
from ray.util.annotations import Deprecated

from ray.train.hf_transformers.transformers_trainer import (
    TransformersTrainer,
)

from ._deprecation_msg import deprecation_msg


@Deprecated(message=deprecation_msg)
class HuggingFaceTrainer(TransformersTrainer):
    # Use __new__ as it is much less likely to be overriden
    # than __init__
    def __new__(cls: type, *args, **kwargs):
        warnings.warn(deprecation_msg, DeprecationWarning)
        return super(HuggingFaceTrainer, cls).__new__(cls)


__all__ = [
    "HuggingFaceTrainer",
]

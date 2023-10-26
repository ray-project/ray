import warnings

from ._deprecation_msg import deprecation_msg
from ray.train.huggingface.transformers.transformers_trainer import TransformersTrainer
from ray.util.annotations import Deprecated


@Deprecated(message=deprecation_msg)
class HuggingFaceTrainer(TransformersTrainer):
    # Use __new__ as it is much less likely to be overriden
    # than __init__
    def __new__(cls: type, *args, **kwargs):
        warnings.warn(deprecation_msg, DeprecationWarning, stacklevel=2)
        return super(HuggingFaceTrainer, cls).__new__(cls, *args, **kwargs)


__all__ = [
    "HuggingFaceTrainer",
]

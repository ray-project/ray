import warnings
from ray.util.annotations import Deprecated

from ray.train.huggingface.transformers.transformers_predictor import (
    TransformersPredictor,
)

from ._deprecation_msg import deprecation_msg


@Deprecated(message=deprecation_msg)
class HuggingFacePredictor(TransformersPredictor):
    # Use __new__ as it is much less likely to be overriden
    # than __init__
    def __new__(cls: type):
        warnings.warn(deprecation_msg, DeprecationWarning)


__all__ = [
    "HuggingFacePredictor",
]

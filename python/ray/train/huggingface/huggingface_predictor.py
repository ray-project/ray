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
    def __new__(cls: type, *args, **kwargs):
        warnings.warn(deprecation_msg, DeprecationWarning, stacklevel=2)
        return super(HuggingFacePredictor, cls).__new__(cls)


__all__ = [
    "HuggingFacePredictor",
]

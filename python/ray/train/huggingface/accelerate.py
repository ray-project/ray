import warnings

deprecation_msg = (
    "`ray.train.huggingface.accelerate` has been renamed to "
    "`ray.train.hf_accelerate`. This import path is left as an alias "
    "but will be removed in the future."
)
warnings.warn(deprecation_msg, DeprecationWarning)

from ray.train.hf_accelerate import *  # noqa

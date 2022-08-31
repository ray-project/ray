from dataclasses import dataclass
import warnings

from ray.air.config import CheckpointConfig
from ray.util.annotations import Deprecated

# Deprecated. Alias of CheckpointConfig for backwards compat
deprecation_message = (
    "`ray.train.checkpoint.CheckpointStrategy` is deprecated and will be removed in "
    "the future. Please use `ray.air.config.CheckpointConfig` "
    "instead."
)


@Deprecated(message=deprecation_message)
@dataclass
class CheckpointStrategy(CheckpointConfig):
    def __post_init__(self):
        warnings.warn(deprecation_message, DeprecationWarning, stacklevel=2)
        super().__post_init__()

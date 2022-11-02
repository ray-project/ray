from dataclasses import dataclass

from ray.air.config import CheckpointConfig
from ray.util.annotations import Deprecated

# Deprecated. Alias of CheckpointConfig for backwards compat
deprecation_message = (
    "`ray.train.checkpoint.CheckpointStrategy` is deprecated in Ray 2.0. "
    "Please use `ray.air.config.CheckpointConfig` instead."
)


@Deprecated(message=deprecation_message)
@dataclass
class CheckpointStrategy(CheckpointConfig):
    def __post_init__(self):
        raise DeprecationWarning(deprecation_message)
        super().__post_init__()

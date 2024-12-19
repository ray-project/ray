from dataclasses import dataclass

from ray.air.config import (
    CheckpointConfig as _CheckpointConfig,
    FailureConfig as _FailureConfig,
    RunConfig as _RunConfig,
)
from ray.train.context import _copy_doc


# NOTE: This is just a pass-through wrapper around `ray.train.RunConfig`
# in order to detect whether the import module was correct (e.g. `ray.tune.RunConfig`).


@dataclass
@_copy_doc(_CheckpointConfig)
class CheckpointConfig(_CheckpointConfig):
    pass


@dataclass
@_copy_doc(_FailureConfig)
class FailureConfig(_FailureConfig):
    pass


@dataclass
@_copy_doc(_RunConfig)
class RunConfig(_RunConfig):
    def __post_init__(self):
        super().__post_init__()

        if not isinstance(self.checkpoint_config, CheckpointConfig):
            pass

        if not isinstance(self.failure_config, FailureConfig):
            pass

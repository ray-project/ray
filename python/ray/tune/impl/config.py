from dataclasses import dataclass

from ray.air.config import (
    CheckpointConfig as _CheckpointConfig,
    FailureConfig as _FailureConfig,
    RunConfig as _RunConfig,
)
from ray.train.constants import (
    V2_MIGRATION_GUIDE_MESSAGE,
    _v2_migration_warnings_enabled,
)
from ray.train.utils import _copy_doc, _log_deprecation_warning

# NOTE: This is just a pass-through wrapper around `ray.tune.RunConfig`
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
        self.checkpoint_config = self.checkpoint_config or CheckpointConfig()
        self.failure_config = self.failure_config or FailureConfig()

        super().__post_init__()

        if not isinstance(self.checkpoint_config, CheckpointConfig):
            if _v2_migration_warnings_enabled():
                _log_deprecation_warning(
                    "The `CheckpointConfig` class should be imported from `ray.tune` "
                    "when passing it to the Tuner. Please update your imports."
                    f"{V2_MIGRATION_GUIDE_MESSAGE}"
                )

        if not isinstance(self.failure_config, FailureConfig):
            if _v2_migration_warnings_enabled():
                _log_deprecation_warning(
                    "The `FailureConfig` class should be imported from `ray.tune` "
                    "when passing it to the Tuner. Please update your imports."
                    f"{V2_MIGRATION_GUIDE_MESSAGE}"
                )

from dataclasses import dataclass
from typing import Optional

from ray.air.config import RunConfig as RunConfigV1
from ray.air.config import ScalingConfig as ScalingConfigV1
from ray.train.v2._internal.constants import _UNSUPPORTED
from ray.train.v2._internal.util import date_str


@dataclass
class ScalingConfig(ScalingConfigV1):
    trainer_resources: Optional[dict] = _UNSUPPORTED

    def __post_init__(self):
        super().__post_init__()

        if self.trainer_resources != _UNSUPPORTED:
            raise NotImplementedError(
                "ScalingConfig(trainer_resources) is not supported."
            )


class RunConfig(RunConfigV1):
    def __post_init__(self):
        super().__post_init__()

        if not self.name:
            self.name = f"ray_train_results-{date_str()}"

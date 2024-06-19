from dataclasses import dataclass
from typing import Optional, Tuple, Union

from ray.air.config import RunConfig as RunConfigV1
from ray.air.config import ScalingConfig as ScalingConfigV1
from ray.train.v2._internal.util import date_str

_UNSUPPORTED_STR = "UNSUPPORTED"


@dataclass
class ScalingConfig(ScalingConfigV1):
    num_workers: Union[int, Tuple[int, int]]

    # TODO: Rewrite docstring, and possibly move this into an ElasticScalingConfig
    # While the worker group is healthy, consider resizing the worker group
    # every `elastic_resize_monitor_interval_s` seconds.
    elastic_resize_monitor_interval_s: float = 60.0

    trainer_resources: Optional[dict] = _UNSUPPORTED_STR

    @property
    def min_workers(self):
        return (
            self.num_workers
            if isinstance(self.num_workers, int)
            else self.num_workers[0]
        )

    @property
    def max_workers(self):
        return (
            self.num_workers
            if isinstance(self.num_workers, int)
            else self.num_workers[1]
        )

    def __post_init__(self):
        super().__post_init__()

        if self.trainer_resources != _UNSUPPORTED_STR:
            raise NotImplementedError(
                "ScalingConfig(trainer_resources) is not supported."
            )

        is_fixed = isinstance(self.num_workers, int)
        is_elastic = (
            isinstance(self.num_workers, tuple)
            and len(self.num_workers) == 2
            and all(isinstance(x, int) for x in self.num_workers)
        )
        if not (is_fixed or is_elastic):
            raise ValueError(
                "ScalingConfig(num_workers) must be an int or a tuple of two ints."
            )

        if self.elastic_resize_monitor_interval_s < 0:
            raise ValueError(
                "ScalingConfig(elastic_resize_monitor_interval_s) must be non-negative."
            )


class RunConfig(RunConfigV1):
    def __post_init__(self):
        super().__post_init__()

        if not self.name:
            self.name = f"ray_train_results-{date_str()}"

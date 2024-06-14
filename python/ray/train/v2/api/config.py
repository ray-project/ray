from dataclasses import dataclass
from typing import Optional, Tuple, Union

from ray.train import RunConfig as RunConfigV1
from ray.train import ScalingConfig as ScalingConfigV1

_UNSUPPORTED_STR = "UNSUPPORTED"


@dataclass
class ScalingConfig(ScalingConfigV1):
    num_workers: Union[int, Tuple[int, int]]

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


class RunConfig(RunConfigV1):
    def __post_init__(self):
        super().__post_init__()

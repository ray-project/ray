import abc
from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from ray.train.v2.api.config import ScalingConfig


@dataclass
class ScalingDecision:
    action: str
    num_workers: Optional[int] = None

    RESIZE = "RESIZE"
    NOOP = "NOOP"

    @classmethod
    def noop(cls):
        return cls(action=cls.NOOP)

    @classmethod
    def resize(cls, num_workers: int):
        return cls(action=cls.RESIZE, num_workers=num_workers)


class ScalingPolicy(abc.ABC):
    def __init__(self, scaling_config: ScalingConfig):
        self.scaling_config = scaling_config

    @classmethod
    def supports_elasticity(cls) -> bool:
        raise NotImplementedError

    def make_decision(self, worker_group_status) -> ScalingDecision:
        raise NotImplementedError

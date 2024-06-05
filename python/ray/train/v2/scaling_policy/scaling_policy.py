import abc
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ray.train.v2.api.config import ScalingConfig


class ScalingDecision:
    pass


class ScalingPolicy(abc.ABC):
    def __init__(self, scaling_config: ScalingConfig):
        self.scaling_config = scaling_config

    def supports_elasticity(self) -> bool: ...

    def make_decision(self, worker_group_status) -> ScalingDecision: ...

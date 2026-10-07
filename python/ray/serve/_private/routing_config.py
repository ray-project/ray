from dataclasses import dataclass
from typing import List

from ray.serve.schema import TargetGroup


@dataclass(frozen=True)
class RoutingConfigVersion:
    """Identifies an ordered routing configuration within a controller epoch."""

    epoch: str
    sequence: int

    def is_at_least(self, other: "RoutingConfigVersion") -> bool:
        """Return whether this version supersedes ``other``."""
        return self.epoch == other.epoch and self.sequence >= other.sequence


@dataclass(frozen=True)
class RoutingConfigSnapshot:
    """Target groups and the version that identifies their desired state."""

    target_groups: List[TargetGroup]
    version: RoutingConfigVersion

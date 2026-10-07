from dataclasses import dataclass
from typing import List

from ray.serve.schema import TargetGroup


@dataclass(frozen=True)
class RoutingConfigVersion:
    """Identifies an ordered routing configuration."""

    generation: int
    sequence: int

    def is_at_least(self, other: "RoutingConfigVersion") -> bool:
        """Return whether this version supersedes ``other``."""
        return (self.generation, self.sequence) >= (
            other.generation,
            other.sequence,
        )


@dataclass(frozen=True)
class RoutingConfigSnapshot:
    """Target groups and the version that identifies their desired state."""

    target_groups: List[TargetGroup]
    version: RoutingConfigVersion

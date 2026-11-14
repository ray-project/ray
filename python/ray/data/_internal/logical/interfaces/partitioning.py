from dataclasses import dataclass
from typing import Optional, Tuple

__all__ = [
    "ClusteredDistribution",
    "HashPartitioning",
]


@dataclass(frozen=True)
class HashPartitioning:
    """Rows are hashed on ``columns`` into ``num_partitions`` partitions.

    Row order and blocks per partition are unspecified.
    """

    columns: Tuple[str, ...]
    num_partitions: int

    def __post_init__(self) -> None:
        assert len(self.columns) > 0, "HashPartitioning requires at least one column"
        assert self.num_partitions > 0, self.num_partitions


@dataclass(frozen=True)
class ClusteredDistribution:
    """Rows with equal ``columns`` must share a partition.

    Any :class:`HashPartitioning` on a subset of ``columns`` satisfies it.
    """

    columns: Tuple[str, ...]

    def __post_init__(self) -> None:
        assert (
            len(self.columns) > 0
        ), "ClusteredDistribution requires at least one column"

    def is_satisfied_by(self, partitioning: Optional[HashPartitioning]) -> bool:
        if partitioning is None:
            return False
        return set(partitioning.columns).issubset(self.columns)

"""Ranker component for operator selection in streaming executor."""

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Generic, List, Protocol, Tuple, TypeVar

from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.execution.resource_manager import ResourceManager

if TYPE_CHECKING:
    from ray.data._internal.execution.streaming_executor_state import Topology

# Protocol for comparable ranking values
class Comparable(Protocol):
    """Protocol for types that can be compared for ranking."""

    def __lt__(self, other: "Comparable") -> bool:
        ...

    def __le__(self, other: "Comparable") -> bool:
        ...

    def __gt__(self, other: "Comparable") -> bool:
        ...

    def __ge__(self, other: "Comparable") -> bool:
        ...

    def __eq__(self, other: "Comparable") -> bool:
        ...


# Generic type for comparable ranking values
RankingValue = TypeVar("RankingValue", bound=Comparable)


class Ranker(ABC, Generic[RankingValue]):
    """Abstract base class for operator ranking strategies."""

    @abstractmethod
    def rank_operator(
        self,
        op: PhysicalOperator,
        topology: "Topology",
        resource_manager: ResourceManager,
    ) -> RankingValue:
        """Rank operator for execution priority.

        Operator to run next is selected as the one with the *smallest* value
        of the lexicographically ordered ranks composed of (in order):

        Args:
            op: Operator to rank
            topology: Current execution topology
            resource_manager: Resource manager for usage information

        Returns:
            Rank (tuple) for operator
        """
        pass

    def rank_operators(
        self,
        ops: List[PhysicalOperator],
        topology: "Topology",
        resource_manager: ResourceManager,
    ) -> List[RankingValue]:

        assert len(ops) > 0
        return [self.rank_operator(op, topology, resource_manager) for op in ops]


class DefaultRanker(Ranker[Tuple[int, int]]):
    """Ranker implementation."""

    def rank_operator(
        self,
        op: PhysicalOperator,
        topology: "Topology",
        resource_manager: ResourceManager,
    ) -> Tuple[int, int]:
        """Computes rank for op. *Lower means better rank*

            1. Whether operator's could be throttled (int)
            2. Operators' object store utilization

        Args:
            op: Operator to rank
            topology: Current execution topology
            resource_manager: Resource manager for usage information

        Returns:
            Rank (tuple) for operator
        """

        throttling_disabled = 0 if op.throttling_disabled() else 1

        return (
            throttling_disabled,
            resource_manager.get_op_usage(op).object_store_memory,
        )

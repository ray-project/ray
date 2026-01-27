from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from ray.data._internal.logical.interfaces import (
    LogicalOperator,
    LogicalOperatorSupportsPredicatePassThrough,
    PredicatePassThroughBehavior,
)
from ray.data._internal.planner.exchange.interfaces import ExchangeTaskSpec
from ray.data._internal.planner.exchange.shuffle_task_spec import ShuffleTaskSpec
from ray.data._internal.planner.exchange.sort_task_spec import SortKey, SortTaskSpec
from ray.data.aggregate import AggregateFn
from ray.data.block import BlockMetadata

if TYPE_CHECKING:
    from ray.data.block import Schema

__all__ = [
    "AbstractAllToAll",
    "Aggregate",
    "RandomShuffle",
    "RandomizeBlocks",
    "Repartition",
    "Sort",
]


@dataclass(frozen=True, repr=False)
class AbstractAllToAll(LogicalOperator):
    """Abstract class for logical operators should be converted to physical
    AllToAllOperator.
    """

    input_op: Optional[LogicalOperator] = None
    ray_remote_args: Optional[Dict[str, Any]] = None
    sub_progress_bar_names: Optional[List[str]] = None

    def __post_init__(self) -> None:
        if not self.input_dependencies and self.input_op is not None:
            object.__setattr__(self, "input_dependencies", (self.input_op,))
        if self.ray_remote_args is None:
            object.__setattr__(self, "ray_remote_args", {})
        super().__post_init__()
        assert self.name is not None
        assert len(self.input_dependencies) == 1, self.input_dependencies


@dataclass(frozen=True, repr=False)
class RandomizeBlocks(AbstractAllToAll, LogicalOperatorSupportsPredicatePassThrough):
    """Logical operator for randomize_block_order."""

    seed: Optional[int] = None

    def __post_init__(self) -> None:
        if self.name is None:
            object.__setattr__(self, "name", "RandomizeBlockOrder")
        super().__post_init__()

    def infer_metadata(self) -> "BlockMetadata":
        assert len(self.input_dependencies) == 1, len(self.input_dependencies)
        assert isinstance(self.input_dependencies[0], LogicalOperator)
        return self.input_dependencies[0].infer_metadata()

    def infer_schema(
        self,
    ) -> Optional["Schema"]:
        assert len(self.input_dependencies) == 1, len(self.input_dependencies)
        assert isinstance(self.input_dependencies[0], LogicalOperator)
        return self.input_dependencies[0].infer_schema()

    def predicate_passthrough_behavior(self) -> PredicatePassThroughBehavior:
        # Randomizing block order doesn't affect filtering correctness
        return PredicatePassThroughBehavior.PASSTHROUGH


@dataclass(frozen=True, repr=False)
class RandomShuffle(AbstractAllToAll, LogicalOperatorSupportsPredicatePassThrough):
    """Logical operator for randomshuffle."""

    seed: Optional[int] = None

    def __post_init__(self) -> None:
        if self.name is None:
            object.__setattr__(self, "name", "RandomShuffle")
        if self.sub_progress_bar_names is None:
            object.__setattr__(
                self,
                "sub_progress_bar_names",
                [
                    ExchangeTaskSpec.MAP_SUB_PROGRESS_BAR_NAME,
                    ExchangeTaskSpec.REDUCE_SUB_PROGRESS_BAR_NAME,
                ],
            )
        super().__post_init__()

    def infer_metadata(self) -> "BlockMetadata":
        assert len(self.input_dependencies) == 1, len(self.input_dependencies)
        assert isinstance(self.input_dependencies[0], LogicalOperator)
        return self.input_dependencies[0].infer_metadata()

    def infer_schema(
        self,
    ) -> Optional["Schema"]:
        assert len(self.input_dependencies) == 1, len(self.input_dependencies)
        assert isinstance(self.input_dependencies[0], LogicalOperator)
        return self.input_dependencies[0].infer_schema()

    def predicate_passthrough_behavior(self) -> PredicatePassThroughBehavior:
        # Random shuffle doesn't affect filtering correctness
        return PredicatePassThroughBehavior.PASSTHROUGH


@dataclass(frozen=True, repr=False)
class Repartition(AbstractAllToAll, LogicalOperatorSupportsPredicatePassThrough):
    """Logical operator for repartition."""

    shuffle: bool = False
    keys: Optional[List[str]] = None
    sort: bool = False

    def __post_init__(self) -> None:
        assert self.num_outputs is not None
        if self.name is None:
            object.__setattr__(self, "name", "Repartition")
        if self.sub_progress_bar_names is None:
            if self.shuffle:
                sub_progress_bar_names = [
                    ExchangeTaskSpec.MAP_SUB_PROGRESS_BAR_NAME,
                    ExchangeTaskSpec.REDUCE_SUB_PROGRESS_BAR_NAME,
                ]
            else:
                sub_progress_bar_names = [
                    ShuffleTaskSpec.SPLIT_REPARTITION_SUB_PROGRESS_BAR_NAME,
                ]
            object.__setattr__(self, "sub_progress_bar_names", sub_progress_bar_names)
        super().__post_init__()

    def infer_metadata(self) -> "BlockMetadata":
        assert len(self.input_dependencies) == 1, len(self.input_dependencies)
        assert isinstance(self.input_dependencies[0], LogicalOperator)
        return self.input_dependencies[0].infer_metadata()

    def infer_schema(
        self,
    ) -> Optional["Schema"]:
        assert len(self.input_dependencies) == 1, len(self.input_dependencies)
        assert isinstance(self.input_dependencies[0], LogicalOperator)
        return self.input_dependencies[0].infer_schema()

    def predicate_passthrough_behavior(self) -> PredicatePassThroughBehavior:
        # Repartition doesn't affect filtering correctness
        return PredicatePassThroughBehavior.PASSTHROUGH


@dataclass(frozen=True, repr=False)
class Sort(AbstractAllToAll, LogicalOperatorSupportsPredicatePassThrough):
    """Logical operator for sort."""

    sort_key: Optional[SortKey] = None
    batch_format: Optional[str] = "default"

    def __post_init__(self) -> None:
        assert self.sort_key is not None
        if self.name is None:
            object.__setattr__(self, "name", "Sort")
        if self.sub_progress_bar_names is None:
            object.__setattr__(
                self,
                "sub_progress_bar_names",
                [
                    SortTaskSpec.SORT_SAMPLE_SUB_PROGRESS_BAR_NAME,
                    ExchangeTaskSpec.MAP_SUB_PROGRESS_BAR_NAME,
                    ExchangeTaskSpec.REDUCE_SUB_PROGRESS_BAR_NAME,
                ],
            )
        super().__post_init__()

    def infer_metadata(self) -> "BlockMetadata":
        assert len(self.input_dependencies) == 1, len(self.input_dependencies)
        assert isinstance(self.input_dependencies[0], LogicalOperator)
        return self.input_dependencies[0].infer_metadata()

    def infer_schema(
        self,
    ) -> Optional["Schema"]:
        assert len(self.input_dependencies) == 1, len(self.input_dependencies)
        assert isinstance(self.input_dependencies[0], LogicalOperator)
        return self.input_dependencies[0].infer_schema()

    def predicate_passthrough_behavior(self) -> PredicatePassThroughBehavior:
        # Sort doesn't affect filtering correctness
        return PredicatePassThroughBehavior.PASSTHROUGH


@dataclass(frozen=True, repr=False)
class Aggregate(AbstractAllToAll):
    """Logical operator for aggregate."""

    key: Optional[str] = None
    aggs: List[AggregateFn] = None  # type: ignore[assignment]
    num_partitions: Optional[int] = None
    batch_format: Optional[str] = "default"

    def __post_init__(self) -> None:
        assert self.aggs is not None
        if self.name is None:
            object.__setattr__(self, "name", "Aggregate")
        if self.sub_progress_bar_names is None:
            object.__setattr__(
                self,
                "sub_progress_bar_names",
                [
                    SortTaskSpec.SORT_SAMPLE_SUB_PROGRESS_BAR_NAME,
                    ExchangeTaskSpec.MAP_SUB_PROGRESS_BAR_NAME,
                    ExchangeTaskSpec.REDUCE_SUB_PROGRESS_BAR_NAME,
                ],
            )
        super().__post_init__()

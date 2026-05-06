from dataclasses import InitVar, dataclass, field, replace
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from ray.data._internal.logical.interfaces import (
    LogicalOperator,
    LogicalOperatorSupportsPredicatePassThrough,
    PredicatePassThroughBehavior,
)
from ray.data._internal.planner.exchange.interfaces import ExchangeTaskSpec
from ray.data._internal.planner.exchange.shuffle_task_spec import ShuffleTaskSpec
from ray.data._internal.planner.exchange.sort_task_spec import SortKey, SortTaskSpec
from ray.data._internal.random_config import RandomSeedConfig
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


@dataclass(frozen=True, repr=False, eq=False, init=False)
class AbstractAllToAll(LogicalOperator):
    """Abstract class for logical operators should be converted to physical
    AllToAllOperator.
    """

    def __init__(
        self,
        input_op: LogicalOperator,
        num_outputs: Optional[int] = None,
        sub_progress_bar_names: Optional[List[str]] = None,
        ray_remote_args: Optional[Dict[str, Any]] = None,
        *,
        name: Optional[str] = None,
    ):
        """Initialize an ``AbstractAllToAll`` logical operator.

        Args:
            input_op: The operator preceding this operator in the plan DAG. The outputs
                of `input_op` will be the inputs to this operator.
            num_outputs: The number of expected output bundles outputted by this
                operator.
            sub_progress_bar_names: Optional sub-stage progress bar names for this
                operator.
            ray_remote_args: Args to provide to :func:`ray.remote`.
            name: Name for this operator. This is the name that will appear when
                inspecting the logical plan of a Dataset.
        """
        super().__init__(
            input_dependencies=[input_op],
        )
        object.__setattr__(self, "num_outputs", num_outputs)
        object.__setattr__(self, "name", name or self.__class__.__name__)
        object.__setattr__(self, "ray_remote_args", ray_remote_args or {})
        object.__setattr__(self, "sub_progress_bar_names", sub_progress_bar_names)


@dataclass(frozen=True, repr=False, eq=False)
class RandomizeBlocks(AbstractAllToAll, LogicalOperatorSupportsPredicatePassThrough):
    """Logical operator for randomize_block_order."""

    seed_config: Optional[RandomSeedConfig] = None
    ray_remote_args: Dict[str, Any] = field(default_factory=dict)
    sub_progress_bar_names: Optional[List[str]] = None
    input_dependencies: List[LogicalOperator] = field(repr=False, kw_only=True)

    def __post_init__(self):
        super().__post_init__()
        assert len(self.input_dependencies) == 1, len(self.input_dependencies)
        if self.seed_config is None:
            object.__setattr__(self, "seed_config", RandomSeedConfig())
        object.__setattr__(self, "name", "RandomizeBlockOrder")
        object.__setattr__(self, "num_outputs", None)

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


@dataclass(frozen=True, repr=False, eq=False)
class RandomShuffle(AbstractAllToAll, LogicalOperatorSupportsPredicatePassThrough):
    """Logical operator for random_shuffle."""

    name: InitVar[str] = "RandomShuffle"
    seed_config: Optional[RandomSeedConfig] = None
    ray_remote_args: Dict[str, Any] = field(default_factory=dict)
    sub_progress_bar_names: Optional[List[str]] = None
    input_dependencies: List[LogicalOperator] = field(repr=False, kw_only=True)

    def __post_init__(self, name: str):
        super().__post_init__()
        assert len(self.input_dependencies) == 1, len(self.input_dependencies)
        if self.seed_config is None:
            object.__setattr__(self, "seed_config", RandomSeedConfig())
        if self.sub_progress_bar_names is None:
            object.__setattr__(
                self,
                "sub_progress_bar_names",
                [
                    ExchangeTaskSpec.MAP_SUB_PROGRESS_BAR_NAME,
                    ExchangeTaskSpec.REDUCE_SUB_PROGRESS_BAR_NAME,
                ],
            )
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "num_outputs", None)

    def _with_new_input_dependencies(
        self, input_dependencies: List[LogicalOperator]
    ) -> LogicalOperator:
        return replace(self, input_dependencies=input_dependencies, name=self.name)

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


@dataclass(frozen=True, repr=False, eq=False)
class Repartition(AbstractAllToAll, LogicalOperatorSupportsPredicatePassThrough):
    """Logical operator for repartition."""

    num_outputs: int
    shuffle: bool = False
    keys: Optional[List[str]] = None
    sort: bool = False
    ray_remote_args: Dict[str, Any] = field(default_factory=dict)
    sub_progress_bar_names: Optional[List[str]] = None
    input_dependencies: List[LogicalOperator] = field(repr=False, kw_only=True)

    def __post_init__(self):
        super().__post_init__()
        assert len(self.input_dependencies) == 1, len(self.input_dependencies)
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
        object.__setattr__(self, "name", self.__class__.__name__)

    def _with_new_input_dependencies(
        self, input_dependencies: List[LogicalOperator]
    ) -> LogicalOperator:
        return replace(
            self,
            input_dependencies=input_dependencies,
            num_outputs=self.num_outputs,
        )

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


@dataclass(frozen=True, repr=False, eq=False)
class Sort(AbstractAllToAll, LogicalOperatorSupportsPredicatePassThrough):
    """Logical operator for sort."""

    sort_key: SortKey
    batch_format: Optional[str] = "default"
    ray_remote_args: Dict[str, Any] = field(default_factory=dict)
    sub_progress_bar_names: Optional[List[str]] = None
    input_dependencies: List[LogicalOperator] = field(repr=False, kw_only=True)

    def __post_init__(self):
        super().__post_init__()
        assert len(self.input_dependencies) == 1, len(self.input_dependencies)
        object.__setattr__(
            self,
            "sub_progress_bar_names",
            [
                SortTaskSpec.SORT_SAMPLE_SUB_PROGRESS_BAR_NAME,
                ExchangeTaskSpec.MAP_SUB_PROGRESS_BAR_NAME,
                ExchangeTaskSpec.REDUCE_SUB_PROGRESS_BAR_NAME,
            ],
        )
        object.__setattr__(self, "name", self.__class__.__name__)
        object.__setattr__(self, "num_outputs", None)

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


@dataclass(frozen=True, repr=False, eq=False)
class Aggregate(AbstractAllToAll):
    """Logical operator for aggregate."""

    key: Optional[str]
    aggs: List[AggregateFn]
    num_partitions: Optional[int] = None
    batch_format: Optional[str] = "default"
    ray_remote_args: Dict[str, Any] = field(default_factory=dict)
    sub_progress_bar_names: Optional[List[str]] = None
    input_dependencies: List[LogicalOperator] = field(repr=False, kw_only=True)

    def __post_init__(self):
        super().__post_init__()
        assert len(self.input_dependencies) == 1, len(self.input_dependencies)
        object.__setattr__(
            self,
            "sub_progress_bar_names",
            [
                SortTaskSpec.SORT_SAMPLE_SUB_PROGRESS_BAR_NAME,
                ExchangeTaskSpec.MAP_SUB_PROGRESS_BAR_NAME,
                ExchangeTaskSpec.REDUCE_SUB_PROGRESS_BAR_NAME,
            ],
        )
        object.__setattr__(self, "name", self.__class__.__name__)
        object.__setattr__(self, "num_outputs", None)

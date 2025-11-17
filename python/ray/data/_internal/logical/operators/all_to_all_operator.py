from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

from ray.data._internal.logical.interfaces import (
    LogicalOperator,
    LogicalOperatorSupportsPredicatePassThrough,
    LogicalOperatorSupportsProjectionPassThrough,
    PredicatePassThroughBehavior,
    ProjectionPassThroughBehavior,
)
from ray.data._internal.planner.exchange.interfaces import ExchangeTaskSpec
from ray.data._internal.planner.exchange.shuffle_task_spec import ShuffleTaskSpec
from ray.data._internal.planner.exchange.sort_task_spec import SortKey, SortTaskSpec
from ray.data.aggregate import AggregateFn
from ray.data.block import BlockMetadata

if TYPE_CHECKING:
    from ray.data._internal.logical.operators.map_operator import Project
    from ray.data.block import Schema


class AbstractAllToAll(LogicalOperator):
    """Abstract class for logical operators should be converted to physical
    AllToAllOperator.
    """

    def __init__(
        self,
        name: str,
        input_op: LogicalOperator,
        num_outputs: Optional[int] = None,
        sub_progress_bar_names: Optional[List[str]] = None,
        ray_remote_args: Optional[Dict[str, Any]] = None,
    ):
        """
        Args:
            name: Name for this operator. This is the name that will appear when
                inspecting the logical plan of a Dataset.
            input_op: The operator preceding this operator in the plan DAG. The outputs
                of `input_op` will be the inputs to this operator.
            num_outputs: The number of expected output bundles outputted by this
                operator.
            ray_remote_args: Args to provide to :func:`ray.remote`.
        """
        super().__init__(name, [input_op], num_outputs=num_outputs)
        self._ray_remote_args = ray_remote_args or {}
        self._sub_progress_bar_names = sub_progress_bar_names


class RandomizeBlocks(
    AbstractAllToAll,
    LogicalOperatorSupportsProjectionPassThrough,
    LogicalOperatorSupportsPredicatePassThrough,
):
    """Logical operator for randomize_block_order."""

    def __init__(
        self,
        input_op: LogicalOperator,
        seed: Optional[int] = None,
    ):
        super().__init__(
            "RandomizeBlockOrder",
            input_op,
        )
        self._seed = seed

    def infer_metadata(self) -> "BlockMetadata":
        assert len(self._input_dependencies) == 1, len(self._input_dependencies)
        assert isinstance(self._input_dependencies[0], LogicalOperator)
        return self._input_dependencies[0].infer_metadata()

    def infer_schema(
        self,
    ) -> Optional["Schema"]:
        assert len(self._input_dependencies) == 1, len(self._input_dependencies)
        assert isinstance(self._input_dependencies[0], LogicalOperator)
        return self._input_dependencies[0].infer_schema()

    def apply_projection_pass_through(
        self,
        renamed_keys: Optional[List[List[str]]],
        upstream_projects: List["Project"],
    ) -> LogicalOperator:
        """Recreate RandomizeBlocks with projection pass-through."""
        return RandomizeBlocks(
            input_op=upstream_projects[0],
            seed=self._seed,
        )

    def projection_passthrough_behavior(self) -> ProjectionPassThroughBehavior:
        return ProjectionPassThroughBehavior.PASSTHROUGH_INTO_BRANCHES

    def predicate_passthrough_behavior(self) -> PredicatePassThroughBehavior:
        # Randomizing block order doesn't affect filtering correctness
        return PredicatePassThroughBehavior.PASSTHROUGH


class RandomShuffle(
    AbstractAllToAll,
    LogicalOperatorSupportsProjectionPassThrough,
    LogicalOperatorSupportsPredicatePassThrough,
):
    """Logical operator for random_shuffle."""

    def __init__(
        self,
        input_op: LogicalOperator,
        name: str = "RandomShuffle",
        seed: Optional[int] = None,
        ray_remote_args: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(
            name,
            input_op,
            sub_progress_bar_names=[
                ExchangeTaskSpec.MAP_SUB_PROGRESS_BAR_NAME,
                ExchangeTaskSpec.REDUCE_SUB_PROGRESS_BAR_NAME,
            ],
            ray_remote_args=ray_remote_args,
        )
        self._seed = seed

    def infer_metadata(self) -> "BlockMetadata":
        assert len(self._input_dependencies) == 1, len(self._input_dependencies)
        assert isinstance(self._input_dependencies[0], LogicalOperator)
        return self._input_dependencies[0].infer_metadata()

    def infer_schema(
        self,
    ) -> Optional["Schema"]:
        assert len(self._input_dependencies) == 1, len(self._input_dependencies)
        assert isinstance(self._input_dependencies[0], LogicalOperator)
        return self._input_dependencies[0].infer_schema()

    def apply_projection_pass_through(
        self,
        renamed_keys: Optional[List[List[str]]],
        upstream_projects: List["Project"],
    ) -> LogicalOperator:
        """Recreate RandomShuffle with projection pass-through."""
        return RandomShuffle(
            input_op=upstream_projects[0],
            name=self._name,
            seed=self._seed,
            ray_remote_args=self._ray_remote_args,
        )

    def projection_passthrough_behavior(self) -> ProjectionPassThroughBehavior:
        return ProjectionPassThroughBehavior.PASSTHROUGH_INTO_BRANCHES

    def predicate_passthrough_behavior(self) -> PredicatePassThroughBehavior:
        # Random shuffle doesn't affect filtering correctness
        return PredicatePassThroughBehavior.PASSTHROUGH


class Repartition(
    AbstractAllToAll,
    LogicalOperatorSupportsProjectionPassThrough,
    LogicalOperatorSupportsPredicatePassThrough,
):
    """Logical operator for repartition."""

    def __init__(
        self,
        input_op: LogicalOperator,
        num_outputs: int,
        shuffle: bool,
        keys: Optional[List[str]] = None,
        sort: bool = False,
    ):
        if shuffle:
            sub_progress_bar_names = [
                ExchangeTaskSpec.MAP_SUB_PROGRESS_BAR_NAME,
                ExchangeTaskSpec.REDUCE_SUB_PROGRESS_BAR_NAME,
            ]
        else:
            sub_progress_bar_names = [
                ShuffleTaskSpec.SPLIT_REPARTITION_SUB_PROGRESS_BAR_NAME,
            ]
        super().__init__(
            "Repartition",
            input_op,
            num_outputs=num_outputs,
            sub_progress_bar_names=sub_progress_bar_names,
        )
        self._shuffle = shuffle
        self._keys = keys
        self._sort = sort

    def infer_metadata(self) -> "BlockMetadata":
        assert len(self._input_dependencies) == 1, len(self._input_dependencies)
        assert isinstance(self._input_dependencies[0], LogicalOperator)
        return self._input_dependencies[0].infer_metadata()

    def infer_schema(
        self,
    ) -> Optional["Schema"]:
        assert len(self._input_dependencies) == 1, len(self._input_dependencies)
        assert isinstance(self._input_dependencies[0], LogicalOperator)
        return self._input_dependencies[0].infer_schema()

    def get_referenced_keys(self) -> Optional[List[List[str]]]:
        """Return partition keys that need to be preserved."""
        return [self._keys] if self._keys else None

    def apply_projection_pass_through(
        self,
        renamed_keys: Optional[List[List[str]]],
        upstream_projects: List["Project"],
    ) -> LogicalOperator:
        """Recreate Repartition with renamed partition keys."""
        return Repartition(
            input_op=upstream_projects[0],
            num_outputs=self._num_outputs,
            shuffle=self._shuffle,
            keys=renamed_keys[0] if (renamed_keys and self._keys is not None) else None,
            sort=self._sort,
        )

    def projection_passthrough_behavior(self) -> ProjectionPassThroughBehavior:
        return ProjectionPassThroughBehavior.PASSTHROUGH_WITH_SUBSTITUTION

    def predicate_passthrough_behavior(self) -> PredicatePassThroughBehavior:
        # Repartition doesn't affect filtering correctness
        return PredicatePassThroughBehavior.PASSTHROUGH


class Sort(
    AbstractAllToAll,
    LogicalOperatorSupportsProjectionPassThrough,
    LogicalOperatorSupportsPredicatePassThrough,
):
    """Logical operator for sort."""

    def __init__(
        self,
        input_op: LogicalOperator,
        sort_key: SortKey,
        batch_format: Optional[str] = "default",
    ):
        super().__init__(
            "Sort",
            input_op,
            sub_progress_bar_names=[
                SortTaskSpec.SORT_SAMPLE_SUB_PROGRESS_BAR_NAME,
                ExchangeTaskSpec.MAP_SUB_PROGRESS_BAR_NAME,
                ExchangeTaskSpec.REDUCE_SUB_PROGRESS_BAR_NAME,
            ],
        )
        self._sort_key = sort_key
        self._batch_format = batch_format

    def infer_metadata(self) -> "BlockMetadata":
        assert len(self._input_dependencies) == 1, len(self._input_dependencies)
        assert isinstance(self._input_dependencies[0], LogicalOperator)
        return self._input_dependencies[0].infer_metadata()

    def infer_schema(
        self,
    ) -> Optional["Schema"]:
        assert len(self._input_dependencies) == 1, len(self._input_dependencies)
        assert isinstance(self._input_dependencies[0], LogicalOperator)
        return self._input_dependencies[0].infer_schema()

    def get_referenced_keys(self) -> Optional[List[List[str]]]:
        """Return sort keys that need to be preserved."""
        return [self._sort_key.get_columns()]

    def apply_projection_pass_through(
        self,
        renamed_keys: Optional[List[List[str]]],
        upstream_projects: List["Project"],
    ) -> LogicalOperator:
        """Recreate Sort with renamed sort keys."""
        # Create new sort key with renamed columns (index into [0] for single-input)
        new_sort_key = SortKey(
            key=renamed_keys[0] if renamed_keys else self._sort_key.get_columns(),
            descending=self._sort_key._descending,
            boundaries=self._sort_key.boundaries,
        )

        # Create Sort with renamed sort key (downstream project handled by caller)
        return Sort(
            input_op=upstream_projects[0],
            sort_key=new_sort_key,
            batch_format=self._batch_format,
        )

    def projection_passthrough_behavior(self) -> ProjectionPassThroughBehavior:
        return ProjectionPassThroughBehavior.PASSTHROUGH_WITH_SUBSTITUTION

    def predicate_passthrough_behavior(self) -> PredicatePassThroughBehavior:
        # Sort doesn't affect filtering correctness
        return PredicatePassThroughBehavior.PASSTHROUGH


class Aggregate(AbstractAllToAll):
    """Logical operator for aggregate."""

    def __init__(
        self,
        input_op: LogicalOperator,
        key: Optional[Union[str, List[str]]],
        aggs: List[AggregateFn],
        num_partitions: Optional[int] = None,
        batch_format: Optional[str] = "default",
    ):
        super().__init__(
            "Aggregate",
            input_op,
            sub_progress_bar_names=[
                SortTaskSpec.SORT_SAMPLE_SUB_PROGRESS_BAR_NAME,
                ExchangeTaskSpec.MAP_SUB_PROGRESS_BAR_NAME,
                ExchangeTaskSpec.REDUCE_SUB_PROGRESS_BAR_NAME,
            ],
        )
        self._key = key
        self._aggs = aggs
        self._num_partitions = num_partitions
        self._batch_format = batch_format

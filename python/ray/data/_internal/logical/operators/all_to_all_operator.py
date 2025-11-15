from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

from ray.data._internal.logical.interfaces import (
    LogicalOperator,
    LogicalOperatorSupportsPredicatePassThrough,
    LogicalOperatorSupportsProjectionPassThrough,
    PredicatePassThroughBehavior,
)
from ray.data._internal.planner.exchange.interfaces import ExchangeTaskSpec
from ray.data._internal.planner.exchange.shuffle_task_spec import ShuffleTaskSpec
from ray.data._internal.planner.exchange.sort_task_spec import SortKey, SortTaskSpec
from ray.data.aggregate import AggregateFn
from ray.data.block import BlockMetadata

if TYPE_CHECKING:

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
        column_rename_map: Dict[str, str],
    ) -> LogicalOperator:

        upstream_project = self._create_upstream_project(
            columns_to_rename=list(column_rename_map.keys()),
            column_rename_map=column_rename_map,
            input_op=self.input_dependencies[0],
        )

        return RandomizeBlocks(
            input_op=upstream_project,
            seed=self._seed,
        )

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
        column_rename_map: Dict[str, str],
    ) -> LogicalOperator:

        upstream_project = self._create_upstream_project(
            columns_to_rename=list(column_rename_map.keys()),
            column_rename_map=column_rename_map,
            input_op=self.input_dependencies[0],
        )

        return RandomShuffle(
            input_op=upstream_project,
            name=self._name,
            seed=self._seed,
            ray_remote_args=self._ray_remote_args,
        )

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

    def apply_projection_pass_through(
        self,
        column_rename_map: Dict[str, str],
    ) -> LogicalOperator:

        # When pushing projections through repartition, we must ensure partition key columns
        # are preserved, even if they're not in the output projection.
        # This is necessary because the repartition operation needs these columns to partition by.

        # Collect all required columns (output columns + partition keys). If the keys are
        # None, that means they are using sort-based repartition.
        columns_to_rename = set(column_rename_map.keys()) | set(self._keys or [])

        upstream_project = self._create_upstream_project(
            columns_to_rename=list(columns_to_rename),
            column_rename_map=column_rename_map,
            input_op=self.input_dependencies[0],
        )

        new_keys: Optional[List[str]] = None
        if self._keys is not None:
            new_keys = self._rename_keys(
                old_keys=self._keys,
                column_rename_map=column_rename_map,
            )

        repartition_op = Repartition(
            input_op=upstream_project,
            num_outputs=self._num_outputs,
            shuffle=self._shuffle,
            keys=new_keys,
            sort=self._sort,
        )

        if len(columns_to_rename) == len(column_rename_map):
            # This means the user selected all of the partition keys, so no
            # we can short-circuit and return just a Repartition.
            return repartition_op
        else:
            # This means the user selected columns that were not part of the
            # partition key. This means we must apply an additional projection.
            return self._create_downstream_project(
                column_rename_map=column_rename_map,
                input_op=repartition_op,
            )

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

    def apply_projection_pass_through(
        self,
        column_rename_map: Dict[str, str],
    ) -> LogicalOperator:

        # When pushing projections through sort, we must ensure sort key columns
        # are preserved, even if they're not in the output projection.
        # This is necessary because the sort operation needs these columns to sort by.

        # Collect all required columns (output columns + sort keys)
        columns_to_rename = set(column_rename_map.keys()) | set(
            self._sort_key.get_columns()
        )

        upstream_project = self._create_upstream_project(
            columns_to_rename=list(columns_to_rename),
            column_rename_map=column_rename_map,
            input_op=self.input_dependencies[0],
        )
        new_columns: List[str] = self._rename_keys(
            old_keys=self._sort_key.get_columns(),
            column_rename_map=column_rename_map,
        )
        new_sort_key = SortKey(
            key=new_columns,
            descending=self._sort_key._descending,
            boundaries=self._sort_key.boundaries,
        )

        sort_op = Sort(
            input_op=upstream_project,
            sort_key=new_sort_key,
            batch_format=self._batch_format,
        )

        if len(columns_to_rename) == len(column_rename_map):
            # This means the user selected all of the partition keys, so
            # we can short-circuit and return just a sort.
            return sort_op
        else:
            # This means the user selected columns that were not part of the
            # partition key. This means we must apply an additional projection.
            return self._create_downstream_project(
                column_rename_map=column_rename_map,
                input_op=sort_op,
            )

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

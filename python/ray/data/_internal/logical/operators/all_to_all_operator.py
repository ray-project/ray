from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

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


class RandomizeBlocks(AbstractAllToAll, LogicalOperatorSupportsPredicatePassThrough):
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

    def predicate_passthrough_behavior(self) -> PredicatePassThroughBehavior:
        # Randomizing block order doesn't affect filtering correctness
        return PredicatePassThroughBehavior.PASSTHROUGH


class RandomShuffle(AbstractAllToAll, LogicalOperatorSupportsPredicatePassThrough):
    """Logical operator for random_shuffle."""

    def __init__(
        self,
        input_op: LogicalOperator,
        name: str = "RandomShuffle",
        num_outputs: Optional[int] = None,
        seed: Optional[int] = None,
        ray_remote_args: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(
            name,
            input_op,
            num_outputs=num_outputs,
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

    def predicate_passthrough_behavior(self) -> PredicatePassThroughBehavior:
        # Random shuffle doesn't affect filtering correctness
        return PredicatePassThroughBehavior.PASSTHROUGH


class Repartition(
    AbstractAllToAll,
    LogicalOperatorSupportsPredicatePassThrough,
):
    """Logical operator for repartition."""

    def __init__(
        self,
        input_op: LogicalOperator,
        num_outputs: int,
        full_shuffle: bool,
        name: str = "Repartition",
        random_permute: bool = False,
        keys: Optional[List[str]] = None,
        sort: bool = False,
    ):
        if full_shuffle:
            sub_progress_bar_names = [
                ExchangeTaskSpec.MAP_SUB_PROGRESS_BAR_NAME,
                ExchangeTaskSpec.REDUCE_SUB_PROGRESS_BAR_NAME,
            ]
        else:
            sub_progress_bar_names = [
                ShuffleTaskSpec.SPLIT_REPARTITION_SUB_PROGRESS_BAR_NAME,
            ]
        super().__init__(
            name,
            input_op,
            num_outputs=num_outputs,
            sub_progress_bar_names=sub_progress_bar_names,
        )
        # If True, performs all-to-all shuffling.
        self._full_shuffle = full_shuffle
        # If True, will randomly permute the order of blocks and rows within blocks.
        self._random_permute = random_permute
        self._keys = keys
        self._sort = sort

    def get_partition_keys(self) -> Optional[List[str]]:
        """Return the partition keys for this repartition operation."""
        return self._keys

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

    def predicate_passthrough_behavior(self) -> PredicatePassThroughBehavior:
        # Repartition doesn't affect filtering correctness
        return PredicatePassThroughBehavior.PASSTHROUGH


class Sort(AbstractAllToAll, LogicalOperatorSupportsPredicatePassThrough):
    """Logical operator for sort."""

    def __init__(
        self,
        input_op: LogicalOperator,
        sort_key: SortKey,
        name: str = "Sort",
        batch_format: Optional[str] = "default",
    ):
        super().__init__(
            name,
            input_op,
            sub_progress_bar_names=[
                SortTaskSpec.SORT_SAMPLE_SUB_PROGRESS_BAR_NAME,
                ExchangeTaskSpec.MAP_SUB_PROGRESS_BAR_NAME,
                ExchangeTaskSpec.REDUCE_SUB_PROGRESS_BAR_NAME,
            ],
        )
        self._sort_key = sort_key
        self._batch_format = batch_format

    def get_partition_keys(self) -> Optional[List[str]]:
        """Return the columns used for sorting as partition keys."""
        return self._sort_key._columns

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
        name: str = "Aggregate",
        num_partitions: Optional[int] = None,
        batch_format: Optional[str] = "default",
    ):
        super().__init__(
            name,
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

    def get_partition_keys(self) -> Optional[List[str]]:
        """Return the groupby keys for this aggregate operation."""
        if self._key is None:
            return None
        elif isinstance(self._key, str):
            return [self._key]
        else:
            return self._key

from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

from ray.data._internal.logical.interfaces import (
    LogicalOperator,
    SupportsPushThrough,
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


class RandomizeBlocks(AbstractAllToAll):
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


class RandomShuffle(AbstractAllToAll, SupportsPushThrough):
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

    def apply_projection(
        self,
        columns: Optional[List[str]],
        column_rename_map: Optional[Dict[str, str]],
    ) -> LogicalOperator:

        upstream_project = self._create_upstream_project(
            columns, column_rename_map, self.input_dependencies[0]
        )

        return RandomShuffle(
            input_op=upstream_project,
            name=self._name,
            seed=self._seed,
            ray_remote_args=self._ray_remote_args,
        )


class Repartition(AbstractAllToAll, SupportsPushThrough):
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

    def get_current_projection(self) -> Optional[List[str]]:
        return self._keys

    def apply_projection(
        self,
        columns: Optional[List[str]],
        column_rename_map: Optional[Dict[str, str]],
    ) -> LogicalOperator:

        upstream_project = self._create_upstream_project(
            columns, column_rename_map, self.input_dependencies[0]
        )

        new_keys = self._rename_projection(column_rename_map)

        return Repartition(
            input_op=upstream_project,
            num_outputs=self._num_outputs,
            shuffle=self._shuffle,
            keys=new_keys,
            sort=self._sort,
        )


class Sort(AbstractAllToAll, SupportsPushThrough):
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

    def apply_projection(
        self,
        columns: Optional[List[str]],
        column_rename_map: Optional[Dict[str, str]],
    ) -> LogicalOperator:

        upstream_project = self._create_upstream_project(
            columns, column_rename_map, self.input_dependencies[0]
        )
        new_columns = self._rename_projection(column_rename_map)
        new_sort_key = SortKey(
            key=new_columns,
            descending=self._sort_key._descending,
            boundaries=self._sort_key.boundaries,
        )
        return Sort(
            input_op=upstream_project,
            sort_key=new_sort_key,
            batch_format=self._batch_format,
        )


class Aggregate(AbstractAllToAll, SupportsPushThrough):
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

    def get_current_projection(self) -> Optional[List[str]]:
        if self._key is None:
            return None
        elif isinstance(self._key, str):
            return [self._key]
        else:
            return self._key

    def apply_projection(
        self,
        columns: Optional[List[str]],
        column_rename_map: Optional[Dict[str, str]],
    ) -> LogicalOperator:

        upstream_project = self._create_upstream_project(
            columns, column_rename_map, self.input_dependencies[0]
        )
        new_columns = self._rename_projection(column_rename_map)
        return Aggregate(
            input_op=upstream_project,
            key=new_columns,
            # TODO(justin): this is broken, need to rename which columns are referenced here.
            aggs=self._aggs,
            num_partitions=self._num_partitions,
            batch_format=self._batch_format,
        )

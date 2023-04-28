from typing import Any, Dict, List, Optional

from ray.data._internal.logical.interfaces import LogicalOperator
from ray.data.aggregate import AggregateFn


class AbstractAllToAll(LogicalOperator):
    """Abstract class for logical operators should be converted to physical
    AllToAllOperator.
    """

    def __init__(
        self,
        name: str,
        input_op: LogicalOperator,
        num_outputs: Optional[int] = None,
        ray_remote_args: Optional[Dict[str, Any]] = None,
    ):
        """
        Args:
            name: Name for this operator. This is the name that will appear when
                inspecting the logical plan of a Datastream.
            input_op: The operator preceding this operator in the plan DAG. The outputs
                of `input_op` will be the inputs to this operator.
            num_outputs: The number of expected output bundles outputted by this
                operator.
            ray_remote_args: Args to provide to ray.remote.
        """
        super().__init__(name, [input_op])
        self._num_outputs = num_outputs
        self._ray_remote_args = ray_remote_args or {}


class RandomizeBlocks(AbstractAllToAll):
    """Logical operator for randomize_block_order."""

    def __init__(
        self,
        input_op: LogicalOperator,
        seed: Optional[int] = None,
    ):
        super().__init__(
            "RandomizeBlocks",
            input_op,
        )
        self._seed = seed


class RandomShuffle(AbstractAllToAll):
    """Logical operator for random_shuffle."""

    def __init__(
        self,
        input_op: LogicalOperator,
        seed: Optional[int] = None,
        num_outputs: Optional[int] = None,
        ray_remote_args: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(
            "RandomShuffle",
            input_op,
            num_outputs=num_outputs,
            ray_remote_args=ray_remote_args,
        )
        self._seed = seed


class Repartition(AbstractAllToAll):
    """Logical operator for repartition."""

    def __init__(
        self,
        input_op: LogicalOperator,
        num_outputs: int,
        shuffle: bool,
    ):
        super().__init__(
            "Repartition",
            input_op,
            num_outputs=num_outputs,
        )
        self._shuffle = shuffle


class Sort(AbstractAllToAll):
    """Logical operator for sort."""

    def __init__(
        self,
        input_op: LogicalOperator,
        key: Optional[str],
        descending: bool,
    ):
        super().__init__(
            "Sort",
            input_op,
        )
        self._key = key
        self._descending = descending


class Aggregate(AbstractAllToAll):
    """Logical operator for aggregate."""

    def __init__(
        self,
        input_op: LogicalOperator,
        key: Optional[str],
        aggs: List[AggregateFn],
    ):
        super().__init__(
            "Aggregate",
            input_op,
        )
        self._key = key
        self._aggs = aggs

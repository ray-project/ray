from typing import Any, Dict, List, Optional, Tuple, Union

from ray.data._internal.execution.interfaces import RefBundle
from ray.data._internal.stats import StatsDict
from ray.data.block import Block, BlockMetadata


class ExchangeTaskSpec:
    """
    An interface to specify the exchange map and reduce tasks.

    Subclasses should implement the `map` and `reduce` static methods.
    `map` method is to transform one input block into multiple output blocks.
    `reduce` is to combine multiple map output blocks. Both methods are
    single-task operations. See `ExchangeScheduler` for how to distribute
    the operations across multiple tasks.

    Any custom arguments for `map` and `reduce` methods should be specified by
    setting `map_args` and `reduce_args`.

    The concept here is similar to the exchange operator described in
    "Volcano - An Extensible and Parallel Query Evaluation System"
    (https://dl.acm.org/doi/10.1109/69.273032).
    """

    MAP_SUB_PROGRESS_BAR_NAME = "Shuffle Map"
    REDUCE_SUB_PROGRESS_BAR_NAME = "Shuffle Reduce"

    def __init__(self, map_args: List[Any] = None, reduce_args: List[Any] = None):
        self._map_args = map_args or []
        self._reduce_args = reduce_args or []
        assert isinstance(self._map_args, list)
        assert isinstance(self._reduce_args, list)

    @staticmethod
    def map(
        idx: int,
        block: Block,
        output_num_blocks: int,
    ) -> List[Union[BlockMetadata, Block]]:
        """
        Map function to be run on each input block.

        Returns list of [BlockMetadata, Block1, Block2, ..., BlockN].
        """
        raise NotImplementedError

    @staticmethod
    def reduce(
        *mapper_outputs: List[Block],
        partial_reduce: bool = False,
    ) -> Tuple[Block, BlockMetadata]:
        """
        Reduce function to be run for each output block.

        Args:
            mapper_outputs: List of map output blocks to reduce.
            partial_reduce: Whether should partially or fully reduce.

        Returns:
            The reduced block and its metadata.
        """
        raise NotImplementedError


class ExchangeTaskScheduler:
    """
    An interface to schedule exchange tasks (`exchange_spec`) for multi-nodes
    execution.
    """

    def __init__(self, exchange_spec: ExchangeTaskSpec):
        """
        Args:
            exchange_spec: The implementation of exchange tasks to execute.
        """
        self._exchange_spec = exchange_spec

    def execute(
        self,
        refs: List[RefBundle],
        output_num_blocks: int,
        map_ray_remote_args: Optional[Dict[str, Any]] = None,
        reduce_ray_remote_args: Optional[Dict[str, Any]] = None,
    ) -> Tuple[List[RefBundle], StatsDict]:
        """
        Execute the exchange tasks on input `refs`.
        """
        raise NotImplementedError

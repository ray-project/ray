import logging
from typing import Any, Dict, List, Optional, Tuple, Union

import ray._private.worker
from ray.air.util.data_batch_conversion import BatchFormat
from ray.data._internal.execution.interfaces import RefBundle
from ray.data._internal.stats import StatsDict
from ray.data._internal.util import convert_bytes_to_human_readable_str
from ray.data.block import Block, BlockMetadata, BlockType
from ray.data.context import DataContext

logger = logging.getLogger(__name__)


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

    @staticmethod
    def _derive_target_block_type(batch_format: str) -> Optional[BlockType]:
        if batch_format == BatchFormat.ARROW:
            return BlockType.ARROW
        elif batch_format == BatchFormat.PANDAS:
            return BlockType.PANDAS
        else:
            # NOTE: Unless desired batch-format is specified, avoid
            #       overriding existing one
            return None


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
        # If driver memory exceeds this threshold, warn the user. For now, this
        # only applies to shuffle ops because most other ops are unlikely to use as
        # much driver memory.
        self.warn_on_driver_memory_usage_bytes: Optional[
            int
        ] = DataContext.get_current().warn_on_driver_memory_usage_bytes

    def execute(
        self,
        refs: List[RefBundle],
        output_num_blocks: int,
        map_ray_remote_args: Optional[Dict[str, Any]] = None,
        reduce_ray_remote_args: Optional[Dict[str, Any]] = None,
        warn_on_driver_memory_usage: Optional[int] = None,
    ) -> Tuple[List[RefBundle], StatsDict]:
        """
        Execute the exchange tasks on input `refs`.
        """
        raise NotImplementedError

    def warn_on_high_local_memory_store_usage(self) -> None:
        ray_core_worker = ray._private.worker.global_worker.core_worker
        local_memory_store_bytes_used = (
            ray_core_worker.get_local_memory_store_bytes_used()
        )
        self.warn_on_driver_memory_usage(
            local_memory_store_bytes_used,
            "More than "
            f"{convert_bytes_to_human_readable_str(local_memory_store_bytes_used)} "
            "of driver memory used to store Ray Data block data and metadata. "
            "This job may exit if driver memory is insufficient.\n\n"
            "This can happen when many tiny blocks are created. "
            "Check the block size using Dataset.stats() and see "
            "https://docs.ray.io/en/latest/data/performance-tips.html"
            " for mitigation.",
        )

    def warn_on_driver_memory_usage(
        self, memory_usage_bytes: int, log_str: str
    ) -> None:
        if self.warn_on_driver_memory_usage_bytes is None:
            return

        if memory_usage_bytes > self.warn_on_driver_memory_usage_bytes:
            logger.warning(log_str)
            # Double the threshold to avoid verbose warnings.
            self.warn_on_driver_memory_usage_bytes = memory_usage_bytes * 2

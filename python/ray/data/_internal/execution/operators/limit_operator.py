import copy
from collections import deque
from typing import Deque, List, Optional, Tuple

import ray
from ray.data._internal.execution.interfaces import PhysicalOperator, RefBundle
from ray.data._internal.execution.operators.base_physical_operator import (
    OneToOneOperator,
)
from ray.data._internal.remote_fn import cached_remote_fn
from ray.data._internal.stats import StatsDict
from ray.data.block import Block, BlockAccessor, BlockMetadata, BlockStats
from ray.data.context import DataContext
from ray.types import ObjectRef


class LimitOperator(OneToOneOperator):
    """Physical operator for limit."""

    def __init__(
        self,
        limit: int,
        input_op: PhysicalOperator,
        data_context: DataContext,
    ):
        self._limit = limit
        self._consumed_rows = 0
        self._buffer: Deque[RefBundle] = deque()
        self._name = f"limit={limit}"
        self._output_blocks_stats: List[BlockStats] = []
        self._cur_output_bundles = 0
        super().__init__(self._name, input_op, data_context, target_max_block_size=None)
        if self._limit <= 0:
            self.mark_execution_finished()

    def _limit_reached(self) -> bool:
        return self._consumed_rows >= self._limit

    def _add_input_inner(self, refs: RefBundle, input_index: int) -> None:
        assert not self.completed()
        assert input_index == 0, input_index
        if self._limit_reached():
            return
        out_blocks: List[ObjectRef[Block]] = []
        out_metadata: List[BlockMetadata] = []
        for block, metadata in refs.blocks:
            num_rows = metadata.num_rows
            assert num_rows is not None
            if self._consumed_rows + num_rows <= self._limit:
                out_blocks.append(block)
                out_metadata.append(metadata)
                self._output_blocks_stats.append(metadata.to_stats())
                self._consumed_rows += num_rows
            else:
                # Slice the last block.
                def slice_fn(block, metadata, num_rows) -> Tuple[Block, BlockMetadata]:
                    block = BlockAccessor.for_block(block).slice(0, num_rows, copy=True)
                    metadata = copy.deepcopy(metadata)
                    metadata.num_rows = num_rows
                    metadata.size_bytes = BlockAccessor.for_block(block).size_bytes()
                    return block, metadata

                block, metadata_ref = cached_remote_fn(
                    slice_fn, num_cpus=0, num_returns=2
                ).remote(
                    block,
                    metadata,
                    self._limit - self._consumed_rows,
                )
                out_blocks.append(block)
                metadata = ray.get(metadata_ref)
                out_metadata.append(metadata)
                self._output_blocks_stats.append(metadata.to_stats())
                self._consumed_rows = self._limit
                break
        self._cur_output_bundles += 1
        out_refs = RefBundle(
            list(zip(out_blocks, out_metadata)),
            owns_blocks=refs.owns_blocks,
            schema=refs.schema,
        )
        self._buffer.append(out_refs)
        self._metrics.on_output_queued(out_refs)
        if self._limit_reached():
            self.mark_execution_finished()

        # We cannot estimate if we have only consumed empty blocks,
        # or if the input dependency's total number of output bundles is unknown.
        num_inputs = self.input_dependencies[0].num_outputs_total()
        if self._consumed_rows > 0 and num_inputs is not None:
            # Estimate number of output bundles
            # Check the case where _limit > # of input rows
            estimated_total_output_rows = min(
                self._limit, self._consumed_rows / self._cur_output_bundles * num_inputs
            )
            # _consumed_rows / _limit is roughly equal to
            # _cur_output_bundles / total output blocks
            self._estimated_num_output_bundles = round(
                estimated_total_output_rows
                / self._consumed_rows
                * self._cur_output_bundles
            )

    def has_next(self) -> bool:
        return len(self._buffer) > 0

    def _get_next_inner(self) -> RefBundle:
        output = self._buffer.popleft()
        self._metrics.on_output_dequeued(output)
        return output

    def get_stats(self) -> StatsDict:
        return {self._name: self._output_blocks_stats}

    def num_outputs_total(self) -> Optional[int]:
        # Before execution is completed, we don't know how many output
        # bundles we will have. We estimate based off the consumption so far.
        if self._execution_finished:
            return self._cur_output_bundles
        return self._estimated_num_output_bundles

    def num_output_rows_total(self) -> Optional[int]:
        # The total number of rows is simply the limit or the number
        # of input rows, whichever is smaller
        input_num_rows = self.input_dependencies[0].num_output_rows_total()
        if input_num_rows is None:
            return None
        return min(self._limit, input_num_rows)

    def throttling_disabled(self) -> bool:
        return True

    def implements_accurate_memory_accounting(self) -> bool:
        return True

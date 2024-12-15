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
from ray.data.block import Block, BlockAccessor, BlockMetadata
from ray.data.context import DataContext
from ray.types import ObjectRef


class OffsetOperator(OneToOneOperator):
    """Physical operator for offset."""

    def __init__(
        self,
        offset: int,
        input_op: PhysicalOperator,
        data_context: DataContext,
    ):
        self._offset = offset
        self._consumed_rows = 0  # includes both skipped and outputted rows
        self._buffer: Deque[RefBundle] = deque()
        self._name = f"offset={offset}"
        self._output_metadata: List[BlockMetadata] = []
        self._cur_output_bundles = 0
        self._total_output_rows = 0
        super().__init__(self._name, input_op, data_context, target_max_block_size=None)

    def _offset_reached(self) -> bool:
        return self._consumed_rows >= self._offset

    def _add_input_inner(self, refs: RefBundle, input_index: int) -> None:
        assert not self.completed()
        assert input_index == 0, input_index
        if self._offset_reached():
            # The offset has been reached,
            # so we can pass through all incoming input blocks.
            out_blocks: List[ObjectRef[Block]] = []
            out_metadata: List[BlockMetadata] = []
            for block, metadata in refs.blocks:
                num_rows = metadata.num_rows
                assert num_rows is not None
                out_blocks.append(block)
                out_metadata.append(metadata)
                self._output_metadata.append(metadata)
                self._consumed_rows += num_rows
            self._cur_output_bundles += 1
            out_refs = RefBundle(
                list(zip(out_blocks, out_metadata)),
                owns_blocks=refs.owns_blocks,
            )
            self._buffer.append(out_refs)
            self._metrics.on_output_queued(out_refs)
        else:
            # The offset has not been reached, so we need to skip some rows.
            out_blocks: List[ObjectRef[Block]] = []
            out_metadata: List[BlockMetadata] = []
            for block, metadata in refs.blocks:
                num_rows = metadata.num_rows
                assert num_rows is not None
                if self._consumed_rows + num_rows <= self._offset:
                    # Skip the entire block.
                    self._consumed_rows += num_rows
                else:
                    # Slice the last block.
                    def slice_fn(
                        block, metadata, num_rows_to_skip
                    ) -> Tuple[Block, BlockMetadata]:
                        assert num_rows_to_skip < metadata.num_rows
                        block = BlockAccessor.for_block(block).slice(
                            num_rows_to_skip, metadata.num_rows, copy=True
                        )
                        metadata = copy.deepcopy(metadata)
                        metadata.num_rows -= num_rows_to_skip
                        metadata.size_bytes = BlockAccessor.for_block(
                            block
                        ).size_bytes()
                        return block, metadata

                    block, metadata_ref = cached_remote_fn(
                        slice_fn, num_cpus=0, num_returns=2
                    ).remote(
                        block,
                        metadata,
                        self._offset - self._consumed_rows,
                    )
                    out_blocks.append(block)
                    metadata = ray.get(metadata_ref)
                    out_metadata.append(metadata)
                    self._output_metadata.append(metadata)
                    self._consumed_rows += num_rows
                    break
            self._cur_output_bundles += 1
            out_refs = RefBundle(
                list(zip(out_blocks, out_metadata)),
                owns_blocks=refs.owns_blocks,
            )
            self._buffer.append(out_refs)
            self._metrics.on_output_queued(out_refs)

        # We cannot estimate if we have only consumed empty blocks,
        # and if the input dependency's total number of output bundles is unknown.
        num_inputs = self.input_dependency.num_outputs_total()
        if self._consumed_rows > 0 and num_inputs is not None:
            # Estimate number of output bundles
            # Check the case where _offset > # of input rows
            estimated_total_output_rows = max(
                0,
                self._consumed_rows / self._cur_output_bundles * num_inputs
                - self._offset,
            )
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
        return {self._name: self._output_metadata}

    def num_outputs_total(self) -> Optional[int]:
        # Before execution is completed, we don't know how many output
        # bundles we will have. We estimate based off the consumption so far.
        if self._execution_completed:
            return self._cur_output_bundles
        return self._estimated_num_output_bundles

    def num_output_rows_total(self) -> Optional[int]:
        # The total number of rows is simply the the number
        # of input rows minus the offset, capped at 0.
        input_num_rows = self.input_dependency.num_output_rows_total()
        if input_num_rows is None:
            return None
        return max(0, input_num_rows - self._offset)

    def throttling_disabled(self) -> bool:
        return True

    def implements_accurate_memory_accounting(self) -> bool:
        return True

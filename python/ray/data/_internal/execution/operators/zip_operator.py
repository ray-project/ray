import collections
import itertools
from typing import TYPE_CHECKING, List, Optional, Tuple

import ray
from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data._internal.execution.interfaces import PhysicalOperator, RefBundle
from ray.data._internal.execution.operators.base_physical_operator import (
    InternalQueueOperatorMixin,
    NAryOperator,
)
from ray.data._internal.remote_fn import cached_remote_fn
from ray.data._internal.split import _split_at_indices
from ray.data._internal.stats import StatsDict
from ray.data.block import (
    Block,
    BlockAccessor,
    BlockExecStats,
    BlockPartition,
    to_stats,
)
from ray.data.context import DataContext

if TYPE_CHECKING:

    from ray.data.block import BlockMetadataWithSchema


class ZipOperator(InternalQueueOperatorMixin, NAryOperator):
    """An operator that zips its inputs together.

    NOTE: the implementation is bulk for now, which materializes all its inputs in
    object store, before starting execution. Should re-implement it as a streaming
    operator in the future.
    """

    def __init__(
        self,
        data_context: DataContext,
        *input_ops: PhysicalOperator,
    ):
        """Create a ZipOperator.

        Args:
            input_ops: Operators generating input data for this operator to zip.
        """
        assert len(input_ops) >= 2
        self._input_buffers: List[collections.deque[RefBundle]] = [
            collections.deque() for _ in range(len(input_ops))
        ]
        self._output_buffer: collections.deque[RefBundle] = collections.deque()
        self._stats: StatsDict = {}
        super().__init__(
            data_context,
            *input_ops,
        )

    def num_outputs_total(self) -> Optional[int]:
        num_outputs = None
        for input_op in self.input_dependencies:
            input_num_outputs = input_op.num_outputs_total()
            if input_num_outputs is None:
                continue
            if num_outputs is None:
                num_outputs = input_num_outputs
            else:
                num_outputs = max(num_outputs, input_num_outputs)
        return num_outputs

    def num_output_rows_total(self) -> Optional[int]:
        num_rows = None
        for input_op in self.input_dependencies:
            input_num_rows = input_op.num_output_rows_total()
            if input_num_rows is None:
                continue
            if num_rows is None:
                num_rows = input_num_rows
            else:
                num_rows = max(num_rows, input_num_rows)
        return num_rows

    def internal_input_queue_num_blocks(self) -> int:
        return sum(
            len(bundle.block_refs) for buf in self._input_buffers for bundle in buf
        )

    def internal_input_queue_num_bytes(self) -> int:
        return sum(bundle.size_bytes() for buf in self._input_buffers for bundle in buf)

    def internal_output_queue_num_blocks(self) -> int:
        return sum(len(bundle.block_refs) for bundle in self._output_buffer)

    def internal_output_queue_num_bytes(self) -> int:
        return sum(bundle.size_bytes() for bundle in self._output_buffer)

    def clear_internal_input_queue(self) -> None:
        """Clear internal input queues."""
        for input_buffer in self._input_buffers:
            while input_buffer:
                bundle = input_buffer.popleft()
                self._metrics.on_input_dequeued(bundle)

    def clear_internal_output_queue(self) -> None:
        """Clear internal output queue."""
        while self._output_buffer:
            bundle = self._output_buffer.popleft()
            self._metrics.on_output_dequeued(bundle)

    def _add_input_inner(self, refs: RefBundle, input_index: int) -> None:
        assert not self.completed()
        assert 0 <= input_index <= len(self._input_dependencies), input_index
        self._input_buffers[input_index].append(refs)
        self._metrics.on_input_queued(refs)

    def all_inputs_done(self) -> None:
        assert len(self._output_buffer) == 0, len(self._output_buffer)

        # Start with the first input buffer
        while self._input_buffers[0]:
            refs = self._input_buffers[0].popleft()
            self._output_buffer.append(refs)
            self._metrics.on_input_dequeued(refs)

        # Process each additional input buffer
        for input_buffer in self._input_buffers[1:]:
            self._output_buffer, self._stats = self._zip(
                self._output_buffer, input_buffer
            )

            # Clear the input buffer AFTER using it in _zip
            while input_buffer:
                refs = input_buffer.popleft()
                self._metrics.on_input_dequeued(refs)

        # Mark outputs as ready
        for ref in self._output_buffer:
            self._metrics.on_output_queued(ref)

        super().all_inputs_done()

    def has_next(self) -> bool:
        return len(self._output_buffer) > 0

    def _get_next_inner(self) -> RefBundle:
        refs = self._output_buffer.popleft()
        self._metrics.on_output_dequeued(refs)
        return refs

    def get_stats(self) -> StatsDict:
        return self._stats

    def implements_accurate_memory_accounting(self):
        return True

    def _zip(
        self,
        left_input: collections.deque[RefBundle],
        right_input: collections.deque[RefBundle],
    ) -> Tuple[collections.deque[RefBundle], StatsDict]:
        """Zip the RefBundles from `left_input` and `right_input` together.

        Zip is done in 2 steps: aligning blocks, and zipping blocks from
        both sides.

        Aligning blocks (optional): check the blocks from `left_input` and
        `right_input` are aligned or not, i.e. if having different number of blocks, or
        having different number of rows in some blocks. If not aligned, repartition the
        smaller input with `_split_at_indices` to align with larger input.

        Zipping blocks: after blocks from both sides are aligned, zip
        blocks from both sides together in parallel.
        """
        left_blocks_with_metadata = []
        for bundle in left_input:
            for block, meta in bundle.blocks:
                left_blocks_with_metadata.append((block, meta))
        right_blocks_with_metadata = []
        for bundle in right_input:
            for block, meta in bundle.blocks:
                right_blocks_with_metadata.append((block, meta))

        left_block_rows, left_block_bytes = self._calculate_blocks_rows_and_bytes(
            left_blocks_with_metadata
        )
        right_block_rows, right_block_bytes = self._calculate_blocks_rows_and_bytes(
            right_blocks_with_metadata
        )

        # Check that both sides have the same number of rows.
        # TODO(Clark): Support different number of rows via user-directed
        # dropping/padding.
        total_left_rows = sum(left_block_rows)
        total_right_rows = sum(right_block_rows)
        if total_left_rows != total_right_rows:
            raise ValueError(
                "Cannot zip datasets of different number of rows: "
                f"{total_left_rows}, {total_right_rows}"
            )

        # Whether the left and right input sides are inverted
        input_side_inverted = False
        if sum(right_block_bytes) > sum(left_block_bytes):
            # Make sure that right side is smaller, so we minimize splitting
            # work when aligning both sides.
            # TODO(Clark): Improve this heuristic for minimizing splitting work,
            # e.g. by generating the splitting plans for each route (via
            # _generate_per_block_split_indices) and choosing the plan that splits
            # the least cumulative bytes.
            left_blocks_with_metadata, right_blocks_with_metadata = (
                right_blocks_with_metadata,
                left_blocks_with_metadata,
            )
            left_block_rows, right_block_rows = right_block_rows, left_block_rows
            input_side_inverted = True

        # Get the split indices that will align both sides.
        indices = list(itertools.accumulate(left_block_rows))
        indices.pop(-1)

        # Split other at the alignment indices, such that for every block from
        # left side, we have a list of blocks from right side that have the same
        # cumulative number of rows as that left block.
        # NOTE: _split_at_indices has a no-op fastpath if the blocks are already
        # aligned.
        aligned_right_blocks_with_metadata = _split_at_indices(
            right_blocks_with_metadata,
            indices,
            block_rows=right_block_rows,
        )
        del right_blocks_with_metadata

        left_blocks = [b for b, _ in left_blocks_with_metadata]
        right_blocks_list = aligned_right_blocks_with_metadata[0]
        del left_blocks_with_metadata, aligned_right_blocks_with_metadata

        zip_one_block = cached_remote_fn(_zip_one_block, num_returns=2)

        output_blocks = []
        output_metadata_schema = []
        for left_block, right_blocks in zip(left_blocks, right_blocks_list):
            # For each block from left side, zip it together with 1 or more blocks from
            # right side. We're guaranteed to have that left_block has the same number
            # of rows as right_blocks has cumulatively.
            res, meta_with_schema = zip_one_block.remote(
                left_block, *right_blocks, inverted=input_side_inverted
            )
            output_blocks.append(res)
            output_metadata_schema.append(meta_with_schema)

        # Early release memory.
        del left_blocks, right_blocks_list

        # TODO(ekl) it might be nice to have a progress bar here.
        output_metadata_schema: List[BlockMetadataWithSchema] = ray.get(
            output_metadata_schema
        )

        output_refs: collections.deque[RefBundle] = collections.deque()
        input_owned = all(b.owns_blocks for b in left_input)
        for block, meta_with_schema in zip(output_blocks, output_metadata_schema):
            output_refs.append(
                RefBundle(
                    [
                        (
                            block,
                            meta_with_schema.metadata,
                        )
                    ],
                    owns_blocks=input_owned,
                    schema=meta_with_schema.schema,
                )
            )
        stats = {self._name: to_stats(output_metadata_schema)}

        # Clean up inputs.
        for ref in left_input:
            ref.destroy_if_owned()
        for ref in right_input:
            ref.destroy_if_owned()

        return output_refs, stats

    def _calculate_blocks_rows_and_bytes(
        self,
        blocks_with_metadata: BlockPartition,
    ) -> Tuple[List[int], List[int]]:
        """Calculate the number of rows and size in bytes for a list of blocks with
        metadata.
        """
        get_num_rows_and_bytes = cached_remote_fn(_get_num_rows_and_bytes)
        block_rows = []
        block_bytes = []
        for block, metadata in blocks_with_metadata:
            if metadata.num_rows is None or metadata.size_bytes is None:
                # Need to fetch number of rows or size in bytes, so just fetch both.
                num_rows, size_bytes = ray.get(get_num_rows_and_bytes.remote(block))
                # Cache on the block metadata.
                metadata.num_rows = num_rows
                metadata.size_bytes = size_bytes
            block_rows.append(metadata.num_rows)
            block_bytes.append(metadata.size_bytes)
        return block_rows, block_bytes


def _zip_one_block(
    block: Block, *other_blocks: Block, inverted: bool = False
) -> Tuple[Block, "BlockMetadataWithSchema"]:
    """Zip together `block` with `other_blocks`."""
    stats = BlockExecStats.builder()
    # Concatenate other blocks.
    # TODO(Clark): Extend BlockAccessor.zip() to work with N other blocks,
    # so we don't need to do this concatenation.
    builder = DelegatingBlockBuilder()
    for other_block in other_blocks:
        builder.add_block(other_block)
    other_block = builder.build()
    if inverted:
        # Swap blocks if ordering was inverted during block alignment splitting.
        block, other_block = other_block, block
    # Zip block and other blocks.
    result = BlockAccessor.for_block(block).zip(other_block)
    from ray.data.block import BlockMetadataWithSchema

    return result, BlockMetadataWithSchema.from_block(result, stats=stats.build())


def _get_num_rows_and_bytes(block: Block) -> Tuple[int, int]:
    block = BlockAccessor.for_block(block)
    return block.num_rows(), block.size_bytes()

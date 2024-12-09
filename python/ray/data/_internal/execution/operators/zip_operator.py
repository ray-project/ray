import itertools
from typing import List, Optional, Tuple

import ray
from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data._internal.execution.interfaces import PhysicalOperator, RefBundle
from ray.data._internal.remote_fn import cached_remote_fn
from ray.data._internal.split import _split_at_indices
from ray.data._internal.stats import StatsDict
from ray.data.block import (
    Block,
    BlockAccessor,
    BlockExecStats,
    BlockMetadata,
    BlockPartition,
)
from ray.data.context import DataContext


class ZipOperator(PhysicalOperator):
    """An operator that zips its inputs together.

    NOTE: the implementation is bulk for now, which materializes all its inputs in
    object store, before starting execution. Should re-implement it as a streaming
    operator in the future.
    """

    def __init__(
        self,
        left_input_op: PhysicalOperator,
        right_input_op: PhysicalOperator,
        data_context: DataContext,
    ):
        """Create a ZipOperator.

        Args:
            left_input_ops: The input operator at left hand side.
            right_input_op: The input operator at right hand side.
        """
        self._left_buffer: List[RefBundle] = []
        self._right_buffer: List[RefBundle] = []
        self._output_buffer: List[RefBundle] = []
        self._stats: StatsDict = {}
        super().__init__(
            "Zip",
            [left_input_op, right_input_op],
            data_context,
            target_max_block_size=None,
        )

    def num_outputs_total(self) -> Optional[int]:
        left_num_outputs = self.input_dependencies[0].num_outputs_total()
        right_num_outputs = self.input_dependencies[1].num_outputs_total()
        if left_num_outputs is not None and right_num_outputs is not None:
            return max(left_num_outputs, right_num_outputs)
        elif left_num_outputs is not None:
            return left_num_outputs
        else:
            return right_num_outputs

    def num_output_rows_total(self) -> Optional[int]:
        left_num_rows = self.input_dependencies[0].num_output_rows_total()
        right_num_rows = self.input_dependencies[1].num_output_rows_total()
        if left_num_rows is not None and right_num_rows is not None:
            return max(left_num_rows, right_num_rows)
        elif left_num_rows is not None:
            return left_num_rows
        else:
            return right_num_rows

    def _add_input_inner(self, refs: RefBundle, input_index: int) -> None:
        assert not self.completed()
        assert input_index == 0 or input_index == 1, input_index
        if input_index == 0:
            self._left_buffer.append(refs)
        else:
            self._right_buffer.append(refs)

    def all_inputs_done(self) -> None:
        self._output_buffer, self._stats = self._zip(
            self._left_buffer, self._right_buffer
        )
        self._left_buffer.clear()
        self._right_buffer.clear()
        super().all_inputs_done()

    def has_next(self) -> bool:
        return len(self._output_buffer) > 0

    def _get_next_inner(self) -> RefBundle:
        return self._output_buffer.pop(0)

    def get_stats(self) -> StatsDict:
        return self._stats

    def _zip(
        self, left_input: List[RefBundle], right_input: List[RefBundle]
    ) -> Tuple[List[RefBundle], StatsDict]:
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
        output_metadata = []
        for left_block, right_blocks in zip(left_blocks, right_blocks_list):
            # For each block from left side, zip it together with 1 or more blocks from
            # right side. We're guaranteed to have that left_block has the same number
            # of rows as right_blocks has cumulatively.
            res, meta = zip_one_block.remote(
                left_block, *right_blocks, inverted=input_side_inverted
            )
            output_blocks.append(res)
            output_metadata.append(meta)

        # Early release memory.
        del left_blocks, right_blocks_list

        # TODO(ekl) it might be nice to have a progress bar here.
        output_metadata = ray.get(output_metadata)
        output_refs = []
        input_owned = all(b.owns_blocks for b in left_input)
        for block, meta in zip(output_blocks, output_metadata):
            output_refs.append(
                RefBundle(
                    [
                        (
                            block,
                            meta,
                        )
                    ],
                    owns_blocks=input_owned,
                )
            )
        stats = {self._name: output_metadata}

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
) -> Tuple[Block, BlockMetadata]:
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
    br = BlockAccessor.for_block(result)
    return result, br.get_metadata(exec_stats=stats.build())


def _get_num_rows_and_bytes(block: Block) -> Tuple[int, int]:
    block = BlockAccessor.for_block(block)
    return block.num_rows(), block.size_bytes()

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
        self._stats: StatsDict = {"Zip": []}
        self._leftover_blocks: List = [[] for _ in range(len(input_ops))]
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

    def internal_queue_size(self) -> int:
        return sum([len(buf) for buf in self._input_buffers])

    def _add_input_inner(self, refs: RefBundle, input_index: int) -> None:
        assert not self.completed()
        assert 0 <= input_index <= len(self._input_dependencies), input_index
        if (
            refs.blocks[0][1].num_rows == 0
        ):  # If the block has 0 rows, don't add it to the input buffer
            return
        self._input_buffers[input_index].append(refs)

        self._metrics.on_input_queued(refs)
        ref, _ = self._zip(self._input_buffers)
        if ref is None:
            return
        self._output_buffer.append(ref)
        self._metrics.on_output_queued(ref)

    def all_inputs_done(self) -> None:
        while self.check_still_has_blocks(self._input_buffers, self._leftover_blocks):
            ref, _ = self._zip(self._input_buffers)
            self._output_buffer.append(ref)
            self._metrics.on_output_queued(ref)
        assert all(len(buffer) == 0 for buffer in self._input_buffers), list(
            self._input_buffers
        )
        assert all(
            len(leftover_blocks) == 0 for leftover_blocks in self._leftover_blocks
        ), "leftover blocks should be empty"
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

    def check_still_has_blocks(
        self,
        input_buffers: List[collections.deque[RefBundle]],
        leftover_blocks: List[BlockPartition],
    ) -> bool:
        """Check if the input buffers or leftover blocks have at least one bundle reference."""

        return not all(len(buffer) == 0 for buffer in input_buffers) or not all(
            len(leftover_blocks) == 0 for leftover_blocks in self._leftover_blocks
        )

    def _zip(
        self, input_buffers: List[collections.deque[RefBundle]]
    ) -> Tuple[RefBundle, StatsDict]:
        """Zip the RefBundles from `input_buffers` together."""
        # 1. Get a ref bundle from each input buffer and extract the blocks from the ref bundles (Also add the rest from previous iteration to block list's front)
        # 2. Calculate the number of rows and bytes for the blocks, and calculate the total number of rows and bytes for all blocks and get the smallest row count, if the smallest row count is 0, add all other block to _leftover_blocks and return
        # 3. Split the blocks into smaller blocks based on the number of rows and bytes, and store the rest somewhere else (the row count from the Bundle might differ)
        # 4. Zip the blocks together
        # 5. Return the ref bundles

        input_length = len(input_buffers)

        # 1. Get a ref bundle from each input buffer (we already checked that each buffer has at least one bundle reference)
        #    and extract the blocks from the ref bundles (Also add the rest from previous iteration to block list's front)
        all_blocks_with_metadata: List[BlockPartition] = [
            [] for _ in range(input_length)
        ]  # [[(ObjectRef[Block], BlockMetadata), ...], ...]
        from_buffer = [False for _ in range(input_length)]
        for idx in range(input_length):
            # Add the rest from previous iteration to block list's front
            if len(self._leftover_blocks[idx]) > 0:
                all_blocks_with_metadata[idx].append(self._leftover_blocks[idx])
            # Add the blocks from the current bundle
            elif len(input_buffers[idx]) > 0:
                from_buffer[idx] = True
                for block, metadata in input_buffers[idx][0].blocks:
                    all_blocks_with_metadata[idx].append((block, metadata))
        if not all(
            [
                len(blocks_with_metadata) > 0
                for blocks_with_metadata in all_blocks_with_metadata
            ]
        ):
            return None, {}
        self._leftover_blocks = [[] for _ in range(input_length)]

        # 2. Calculate the number of rows and bytes for the blocks, and calculate the total number of rows and bytes for all blocks and get the smallest row count
        block_rows_for_each_bundle: List[
            List[int]
        ] = []  # [[block1_rows, block2_rows, ...], ...]
        smallest_row_count = float("inf")
        smallest_row_count_bundle_idx = -1
        for idx, blocks_with_metadata in enumerate(all_blocks_with_metadata):
            block_rows, _ = self._calculate_blocks_rows_and_bytes(blocks_with_metadata)
            block_rows_for_each_bundle.append(block_rows)
            total_block_rows = sum(block_rows)
            if total_block_rows < smallest_row_count:
                smallest_row_count = total_block_rows
                smallest_row_count_bundle_idx = idx
        if smallest_row_count == float("inf"):
            for idx, blocks_with_metadata in enumerate(all_blocks_with_metadata):
                self._leftover_blocks[
                    idx
                ] = blocks_with_metadata  # Add block partition to leftover blocks since we can't zip anything if the smallest row count is 0
            return None, {}

        # 3. Split the blocks into smaller blocks based on the number of rows and bytes, and store the rest somewhere else (the row count from the Bundle might differ)
        indices = list(
            itertools.accumulate(
                block_rows_for_each_bundle[smallest_row_count_bundle_idx]
            )
        )  # we dont pop the last index because the row num may not be the same throughout the inputs we handle that later
        aligned_all_blocks_with_metadata = [[] for _ in range(input_length)]
        for idx, blocks_with_metadata in enumerate(all_blocks_with_metadata):
            aligned_all_blocks_with_metadata[idx] = _split_at_indices(
                blocks_with_metadata,
                indices,
                block_rows_for_each_bundle[idx],
            )
        del all_blocks_with_metadata
        for idx, blocks_with_metadata in enumerate(aligned_all_blocks_with_metadata):
            block_list = aligned_all_blocks_with_metadata[idx][0].pop()
            metadata_list = aligned_all_blocks_with_metadata[idx][1].pop()
            if len(block_list) > 0:
                self._leftover_blocks[idx] = (block_list[0], metadata_list[0])
            # remove the last block
            # [input_idx][block or metadata][object refs]

        # 4. Zip the blocks together
        zip_blocks = cached_remote_fn(_zip_blocks, num_returns=2)

        output_blocks = []
        output_metadata_schema = []
        blocks_to_zip = []
        for blocks_metadata in aligned_all_blocks_with_metadata:
            for blocks in blocks_metadata[0]:
                blocks_to_zip.extend(blocks)

        output_blocks, output_metadata_schema = zip_blocks.remote(blocks_to_zip)
        output_metadata_schema = ray.get(output_metadata_schema)
        input_owned = all(b.owns_blocks for b in input_buffers[0])
        output_ref = RefBundle(
            [(output_blocks, output_metadata_schema.metadata)],
            owns_blocks=input_owned,
            schema=output_metadata_schema.schema,
        )
        for idx in range(input_length):
            if from_buffer[idx]:
                ref = input_buffers[idx].popleft()
                ref.destroy_if_owned()

        stats = {self._name: to_stats([output_metadata_schema])}

        return output_ref, stats


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


def _zip_blocks(blocks: List[Block]) -> Tuple[Block, "BlockMetadataWithSchema"]:
    """Zip the blocks together using _zip_one_block in a chain."""
    if len(blocks) == 0:
        raise ValueError("Cannot zip empty list of blocks")

    if len(blocks) == 1:
        from ray.data.block import BlockMetadataWithSchema

        return blocks[0], BlockMetadataWithSchema.from_block(blocks[0])

    # Chain zip: start with first block, then zip with each subsequent block
    blocks = ray.get(blocks)
    result = blocks[0]
    for i in range(1, len(blocks)):
        # Zip current result with the next block
        result, meta_with_schema = _zip_one_block(result, blocks[i], inverted=False)

    return result, meta_with_schema


def _get_num_rows_and_bytes(block: Block) -> Tuple[int, int]:
    block = BlockAccessor.for_block(block)
    return block.num_rows(), block.size_bytes()

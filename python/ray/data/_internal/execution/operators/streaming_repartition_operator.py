import math
from collections import deque
from typing import Deque, List, Optional, Tuple

import ray
from ray.data._internal.execution.interfaces import PhysicalOperator, RefBundle
from ray.data._internal.execution.operators.base_physical_operator import (
    OneToOneOperator,
)
from ray.data._internal.remote_fn import cached_remote_fn
from ray.data._internal.stats import StatsDict
from ray.data.block import (
    Block,
    BlockAccessor,
    BlockMetadata,
    BlockStats,
    _take_first_non_empty_schema,
)
from ray.data.context import DataContext
from ray.types import ObjectRef


def _split_meta(
    m: BlockMetadata, left_size: int
) -> Tuple[BlockMetadata, BlockMetadata]:
    left_bytes = int(math.floor(m.size_bytes * (left_size / m.num_rows)))
    left = BlockMetadata(
        num_rows=left_size,
        size_bytes=left_bytes,
        input_files=m.input_files,
        exec_stats=None,
    )
    right = BlockMetadata(
        num_rows=m.num_rows - left_size,
        size_bytes=m.size_bytes - left_bytes,
        input_files=m.input_files,
        exec_stats=None,
    )
    return left, right


def _split_single_block(b: Block, left_size: int) -> Tuple[Block, Block]:
    acc = BlockAccessor.for_block(b)
    left = acc.slice(0, left_size)
    right = acc.slice(left_size, acc.num_rows())
    assert BlockAccessor.for_block(left).num_rows() == left_size
    assert BlockAccessor.for_block(right).num_rows() == (acc.num_rows() - left_size)
    return left, right


def _merge_blocks(blocks: List[Block]) -> Tuple[Block, BlockMetadata]:
    # Merge a list of blocks into a single block and compute its metadata.
    from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder

    blocks = [ray.get(b) if isinstance(b, ray.ObjectRef) else b for b in blocks]
    builder = DelegatingBlockBuilder()
    for b in blocks:
        builder.add_block(b)
    out = builder.build()
    acc = BlockAccessor.for_block(out)
    meta = BlockMetadata(
        num_rows=acc.num_rows(),
        size_bytes=acc.size_bytes(),
        input_files=[],
        exec_stats=None,
    )
    return out, meta


_split_block = cached_remote_fn(_split_single_block, num_returns=2)
_merge_blocks_remote = cached_remote_fn(_merge_blocks, num_returns=2)


class StreamingRepartitionOperator(OneToOneOperator):
    """Streaming repartition operator producing target-rows blocks.

    - Buffers incoming bundles until reaching `target_num_rows_per_block` rows.
    - When enough rows are buffered, merges them into a single block via a remote task
      and enqueues the result as a RefBundle.
    - If an incoming block would overflow the target, it is split; the left fill
      completes a block (merged with the buffer), and the right remainder is buffered.
    - At EOF, flushes any remaining full-size blocks, then emits the final partial
      block if present.
    """

    def __init__(
        self,
        target_num_rows_per_block: int,
        input_op: PhysicalOperator,
        data_context: DataContext,
    ):
        name = f"StreamingRepartition={target_num_rows_per_block}"
        super().__init__(name, input_op, data_context)
        self._target_num_rows_per_block = target_num_rows_per_block

        # Internal buffer of input bundles not yet merged.
        self._buffer: List[RefBundle] = []
        self._rows_in_buffer: int = 0

        # Output queue of ready bundles.
        self._output_queue: Deque[RefBundle] = deque()
        self._output_blocks_stats: List[BlockStats] = []

    def _add_input_inner(self, refs: RefBundle, input_index: int) -> None:
        assert input_index == 0, input_index
        assert not self.completed()
        in_rows = refs.num_rows()
        if in_rows is None:
            raise ValueError(
                "StreamingRepartition requires bundles with known row count"
            )

        # Process the incoming bundle, possibly splitting it to fill targets.
        incoming: Optional[RefBundle] = refs
        while incoming is not None and incoming.num_rows() > 0:
            # If buffer already aligns with a full block, flush before adding more.
            while self._rows_in_buffer >= self._target_num_rows_per_block:
                self._flush_and_enqueue(self._target_num_rows_per_block)

            need = self._target_num_rows_per_block - self._rows_in_buffer
            nrow_in = incoming.num_rows()

            if nrow_in <= need:
                # Buffer the entire incoming bundle.
                self._buffer.append(incoming)
                self._rows_in_buffer += nrow_in
                self._metrics.on_input_queued(incoming)
                incoming = None
                # If now full, flush.
                if self._rows_in_buffer >= self._target_num_rows_per_block:
                    self._flush_and_enqueue(self._target_num_rows_per_block)
            else:
                # Split incoming to fill the target, buffer left, flush, keep right.
                left, right = self._split_bundle(incoming, need)
                self._buffer.append(left)
                self._rows_in_buffer += left.num_rows()
                self._metrics.on_input_queued(left)
                # Flush exactly one target-sized block.
                self._flush_and_enqueue(self._target_num_rows_per_block)
                # Continue with the remainder.
                incoming = right

    def all_inputs_done(self) -> None:
        # Flush all full target-sized blocks first.
        while self._rows_in_buffer >= self._target_num_rows_per_block:
            self._flush_and_enqueue(self._target_num_rows_per_block)

        # Emit remaining partial block if any rows left.
        if self._rows_in_buffer > 0:
            self._flush_and_enqueue(self._rows_in_buffer)

        super().all_inputs_done()

    def has_next(self) -> bool:
        return len(self._output_queue) > 0

    def _get_next_inner(self) -> RefBundle:
        out = self._output_queue.popleft()
        self._metrics.on_output_dequeued(out)
        return out

    def get_stats(self) -> StatsDict:
        return {self.name: self._output_blocks_stats}

    def throttling_disabled(self) -> bool:
        return True

    def implements_accurate_memory_accounting(self) -> bool:
        return True

    # Internal helpers
    def _flush_and_enqueue(self, nrow: int) -> None:
        """Take exactly `nrow` rows from buffer, merge into a single block remotely,
        and enqueue the resulting RefBundle.
        """
        assert nrow > 0
        bundles = self._split_from_buffer(nrow)

        # Flatten blocks and choose a schema for the output bundle.
        blocks: List[ObjectRef[Block]] = []
        metas: List[BlockMetadata] = []
        for b in bundles:
            for blk, meta in b.blocks:
                blocks.append(blk)
                metas.append(meta)
        # Choose the first non-empty schema from the contributing bundles.
        schema = _take_first_non_empty_schema(b.schema for b in bundles)

        # Submit remote merge task.
        # NOTE: We call `.remote` with the concrete blocks; Ray handles passing refs.
        merged_block_ref, meta_ref = _merge_blocks_remote.options(num_cpus=0).remote(
            blocks
        )
        metadata: BlockMetadata = ray.get(meta_ref)
        self._output_blocks_stats.append(metadata.to_stats())

        out_refs = RefBundle(
            [(merged_block_ref, metadata)],
            owns_blocks=True,
            schema=schema,
        )
        self._output_queue.append(out_refs)
        self._metrics.on_output_queued(out_refs)
        self._rows_in_buffer -= nrow
        assert self._rows_in_buffer >= 0

    def _split_from_buffer(self, nrow: int) -> List[RefBundle]:
        """Remove and return bundles from buffer totaling exactly `nrow` rows.

        If needed, splits the last bundle to satisfy the row count and re-queues the
        right remainder back into the buffer.
        """
        out: List[RefBundle] = []
        acc = 0
        while acc < nrow:
            b = self._buffer.pop()
            self._metrics.on_input_dequeued(b)
            bn = b.num_rows()
            assert bn is not None
            if acc + bn <= nrow:
                out.append(b)
                acc += bn
            else:
                left, right = self._split_bundle(b, nrow - acc)
                out.append(left)
                acc += left.num_rows()
                self._buffer.append(right)
                self._metrics.on_input_queued(right)
                assert acc == nrow, (acc, nrow)

        # Sanity check
        assert sum(b.num_rows() for b in out) == nrow, (acc, nrow)
        return out

    def _split_bundle(
        self, bundle: RefBundle, left_size: int
    ) -> Tuple[RefBundle, RefBundle]:
        """Split a RefBundle into left/right bundles with `left_size` rows on left.

        Splits only the last block that overflows the boundary via a remote task to
        avoid fetching data locally.
        """
        left_blocks: List[ObjectRef[Block]] = []
        left_meta: List[BlockMetadata] = []
        right_blocks: List[ObjectRef[Block]] = []
        right_meta: List[BlockMetadata] = []

        acc = 0
        for b, m in bundle.blocks:
            assert m.num_rows is not None
            if acc >= left_size:
                right_blocks.append(b)
                right_meta.append(m)
                continue
            if acc + m.num_rows <= left_size:
                left_blocks.append(b)
                left_meta.append(m)
                acc += m.num_rows
            else:
                lm, rm = _split_meta(m, left_size - acc)
                lb, rb = _split_block.options(num_cpus=0).remote(b, left_size - acc)
                left_blocks.append(lb)
                left_meta.append(lm)
                right_blocks.append(rb)
                right_meta.append(rm)
                acc += lm.num_rows
                assert acc == left_size

        left = RefBundle(
            list(zip(left_blocks, left_meta)),
            owns_blocks=bundle.owns_blocks,
            schema=bundle.schema,
        )
        right = RefBundle(
            list(zip(right_blocks, right_meta)),
            owns_blocks=bundle.owns_blocks,
            schema=bundle.schema,
        )
        assert left.num_rows() == left_size
        assert left.num_rows() + right.num_rows() == bundle.num_rows()
        return left, right

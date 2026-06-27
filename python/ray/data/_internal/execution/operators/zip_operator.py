import collections
from typing import TYPE_CHECKING, List, Optional, Tuple

from typing_extensions import override

import ray
from ray.data._internal.execution.bundle_queue import BaseBundleQueue, FIFOBundleQueue
from ray.data._internal.execution.interfaces import (
    BlockEntry,
    PhysicalOperator,
    RefBundle,
)
from ray.data._internal.execution.operators.base_physical_operator import (
    InternalQueueOperatorMixin,
    NAryOperator,
)
from ray.data._internal.remote_fn import cached_remote_fn
from ray.data._internal.stats import StatsDict
from ray.data.block import (
    Block,
    BlockAccessor,
    BlockExecStats,
    to_stats,
)
from ray.data.context import DataContext

if TYPE_CHECKING:

    from ray.data.block import BlockMetadataWithSchema


class ZipOperator(InternalQueueOperatorMixin, NAryOperator):
    """An operator that zips its inputs together in a streaming fashion.

    Processes blocks incrementally as they arrive from all inputs,
    aligning rows by splitting blocks as needed.
    """

    def __init__(
        self,
        data_context: DataContext,
        *input_ops: PhysicalOperator,
    ):
        """Create a ZipOperator.

        Args:
            data_context: The :class:`DataContext` to use for this operator.
            *input_ops: Operators generating input data for this operator to zip.
        """
        assert len(input_ops) >= 2
        n = len(input_ops)
        # Bundles received from each input, before being staged into block deques.
        self._input_buffers: List[FIFOBundleQueue] = [
            FIFOBundleQueue() for _ in range(n)
        ]
        self._output_buffer: FIFOBundleQueue = FIFOBundleQueue()
        # Per-input staged blocks as (block_ref, num_rows) awaiting alignment.
        self._block_deques: List[collections.deque] = [
            collections.deque() for _ in range(n)
        ]
        # Per-input remainder left over after a block was split for alignment.
        self._leftovers: List[Optional[Tuple[ray.ObjectRef, int]]] = [None] * n
        self._stats: StatsDict = {}
        self._output_metadata_list: List["BlockMetadataWithSchema"] = []
        super().__init__(
            data_context,
            *input_ops,
        )

    @property
    @override
    def _input_queues(self) -> List["BaseBundleQueue"]:
        return self._input_buffers

    @property
    @override
    def _output_queues(self) -> List["BaseBundleQueue"]:
        return [self._output_buffer]

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

    def _add_input_inner(self, refs: RefBundle, input_index: int) -> None:
        assert not self.has_completed()
        assert 0 <= input_index < len(self._input_dependencies), input_index
        self._input_buffers[input_index].add(refs)
        self._metrics.on_input_queued(refs, input_index=input_index)
        self._try_output()

    def all_inputs_done(self) -> None:
        self._try_output()

        # By now the executor guarantees every input has been fully delivered, so
        # all blocks are present. If the inputs had equal row counts, alignment
        # consumed everything; any remainder means the row counts differed.
        has_leftover = any(leftover is not None for leftover in self._leftovers)
        has_remaining = any(len(d) > 0 for d in self._block_deques)
        has_buffered = any(buf.has_next() for buf in self._input_buffers)
        if has_leftover or has_remaining or has_buffered:
            raise ValueError("Cannot zip datasets of different number of rows")

        self._stats = {self._name: to_stats(self._output_metadata_list)}
        super().all_inputs_done()

    def has_next(self) -> bool:
        return len(self._output_buffer) > 0

    def _get_next_inner(self) -> RefBundle:
        refs = self._output_buffer.get_next()
        self._metrics.on_output_dequeued(refs)
        return refs

    def get_stats(self) -> StatsDict:
        return self._stats

    def throttling_disabled(self) -> bool:
        return False

    def _fill_block_deque(self, input_index: int) -> None:
        """Transfer one bundle from the input buffer into the block deque."""
        if not self._input_buffers[input_index].has_next():
            return
        bundle = self._input_buffers[input_index].get_next()
        self._metrics.on_input_dequeued(bundle, input_index=input_index)
        for entry in bundle.blocks:
            num_rows = entry.metadata.num_rows
            if num_rows is None:
                get_num_rows_fn = cached_remote_fn(_get_num_rows)
                num_rows = ray.get(get_num_rows_fn.remote(entry.ref))
            self._block_deques[input_index].append((entry.ref, num_rows))

    def _has_data(self, input_index: int) -> bool:
        """Check if an input has data available (leftover, deque, or buffer)."""
        if self._leftovers[input_index] is not None:
            return True
        if len(self._block_deques[input_index]) > 0:
            return True
        self._fill_block_deque(input_index)
        return len(self._block_deques[input_index]) > 0

    def _has_data_from_all_inputs(self) -> bool:
        return all(self._has_data(i) for i in range(len(self._input_buffers)))

    def _pop_next_block(self, input_index: int) -> Tuple[ray.ObjectRef, int]:
        """Get the next block from an input, checking the leftover slot first."""
        if self._leftovers[input_index] is not None:
            ref, num_rows = self._leftovers[input_index]
            self._leftovers[input_index] = None
            return ref, num_rows
        if not self._block_deques[input_index]:
            self._fill_block_deque(input_index)
        return self._block_deques[input_index].popleft()

    def _try_output(self) -> None:
        """Process blocks from all inputs and produce zipped output blocks.

        When blocks are available from every input, takes one block from each,
        aligns them to the minimum row count (splitting larger blocks and
        storing the remainder as leftovers), then zips the aligned blocks
        together. Repeats until some input has no data available.
        """
        label_selector = self.data_context.execution_options.label_selector

        split_fn = cached_remote_fn(_split_block_at_row, num_returns=2)
        if label_selector:
            split_fn = split_fn.options(label_selector=label_selector)

        zip_fn = cached_remote_fn(_zip_n_blocks, num_returns=2)
        if label_selector:
            zip_fn = zip_fn.options(label_selector=label_selector)

        while self._has_data_from_all_inputs():
            block_refs = []
            block_rows = []
            for i in range(len(self._input_buffers)):
                ref, num_rows = self._pop_next_block(i)
                block_refs.append(ref)
                block_rows.append(num_rows)

            min_rows = min(block_rows)
            if min_rows == 0:
                # Some block is empty; carry the non-empty ones forward and skip.
                for i in range(len(block_refs)):
                    if block_rows[i] > 0:
                        self._leftovers[i] = (block_refs[i], block_rows[i])
                continue

            # Align blocks to min_rows by splitting any larger blocks, carrying
            # the tail forward as a leftover for the next alignment round.
            aligned_refs = []
            for i in range(len(block_refs)):
                if block_rows[i] == min_rows:
                    aligned_refs.append(block_refs[i])
                else:
                    head_ref, tail_ref = split_fn.remote(block_refs[i], min_rows)
                    aligned_refs.append(head_ref)
                    self._leftovers[i] = (tail_ref, block_rows[i] - min_rows)

            result_ref, meta_ref = zip_fn.remote(*aligned_refs)
            meta_with_schema: "BlockMetadataWithSchema" = ray.get(meta_ref)
            self._output_metadata_list.append(meta_with_schema)

            output = RefBundle(
                [BlockEntry(result_ref, meta_with_schema.metadata)],
                owns_blocks=True,
                schema=meta_with_schema.schema,
            )
            self._output_buffer.add(output)
            self._metrics.on_output_queued(output)


def _split_block_at_row(block: Block, row_index: int) -> Tuple[Block, Block]:
    """Split a block into head ``[0, row_index)`` and tail ``[row_index, end)``."""
    accessor = BlockAccessor.for_block(block)
    head = accessor.slice(0, row_index)
    tail = accessor.slice(row_index, accessor.num_rows())
    return head, tail


def _zip_n_blocks(
    *blocks: Block,
) -> Tuple[Block, "BlockMetadataWithSchema"]:
    """Zip N blocks together by merging their columns."""
    stats = BlockExecStats.builder()
    result = blocks[0]
    for other_block in blocks[1:]:
        result = BlockAccessor.for_block(result).zip(other_block)
    from ray.data.block import BlockMetadataWithSchema

    return result, BlockMetadataWithSchema.from_block(
        result, block_exec_stats=stats.build()
    )


def _get_num_rows(block: Block) -> int:
    return BlockAccessor.for_block(block).num_rows()

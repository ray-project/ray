import collections
from typing import TYPE_CHECKING, Dict, Iterator, List, Optional, Tuple, Union

from typing_extensions import override

import ray
from ray.data._internal.execution.bundle_queue import (
    BaseBundleQueue,
    FIFOBundleQueue,
    ReorderingBundleQueue,
)
from ray.data._internal.execution.interfaces import (
    PhysicalOperator,
    RefBundle,
)
from ray.data._internal.execution.interfaces.physical_operator import (
    DataOpTask,
    MetadataOpTask,
    OpTask,
)
from ray.data._internal.execution.operators.base_physical_operator import (
    InternalQueueOperatorMixin,
    NAryOperator,
)
from ray.data._internal.execution.util import yield_block_with_stats
from ray.data._internal.remote_fn import cached_remote_fn
from ray.data._internal.stats import StatsDict
from ray.data.block import (
    Block,
    BlockAccessor,
    BlockExecStats,
    BlockStats,
    to_stats,
)
from ray.data.context import DataContext

if TYPE_CHECKING:

    from ray.data.block import BlockMetadataWithSchema


class ZipOperator(InternalQueueOperatorMixin, NAryOperator):
    """An operator that zips its inputs together in a streaming fashion.

    Blocks are processed incrementally as they arrive from all inputs. Whenever
    a block is available from every input, the operator takes one block from
    each, aligns them to the minimum row count (splitting larger blocks and
    carrying the remainder forward as a "leftover"), and submits an asynchronous
    Ray task that zips the aligned blocks together.

    All remote work (splitting and zipping) is submitted as :class:`DataOpTask`s
    and surfaced through :meth:`get_active_tasks`, so the streaming executor drives
    them without ever blocking its scheduling loop on ``ray.get``.
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
        self._input_buffers: List[FIFOBundleQueue] = [
            FIFOBundleQueue() for _ in range(n)
        ]
        self._output_buffer: ReorderingBundleQueue = ReorderingBundleQueue()
        self._staging: List[collections.deque] = [collections.deque() for _ in range(n)]
        self._block_deques: List[collections.deque] = [
            collections.deque() for _ in range(n)
        ]
        self._leftovers: List[Optional[Tuple[ray.ObjectRef, int]]] = [None] * n
        self._awaiting_count: List[bool] = [False] * n
        self._data_tasks: Dict[int, DataOpTask] = {}

        self._next_task_idx: int = 0
        self._pending_count_tasks: Dict[int, MetadataOpTask] = {}
        self._next_meta_task_idx: int = 0

        self._inputs_fully_delivered: bool = False

        self._output_blocks_stats: List[BlockStats] = []
        self._stats: StatsDict = {}
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
        self._dispatch_ready_zips()

    def all_inputs_done(self) -> None:
        self._inputs_fully_delivered = True
        self._dispatch_ready_zips()
        # If row-count tasks are pending, the
        # check is deferred to the last one's completion callback instead.
        self._validate_if_settled()
        super().all_inputs_done()

    def _validate_if_settled(self) -> None:
        """
        Raise if inputs have unequal row counts, once all inputs are delivered.
        """
        if not self._inputs_fully_delivered:
            return
        # Defer only while row counts are still being fetched asynchronously; the
        # check re-runs from the row-count task's completion callback. We must NOT
        # defer on staged/buffered blocks here: once inputs are fully delivered
        # and nothing is in flight, `_dispatch_ready_zips` has consumed everything
        # it can, so any block still left in a buffer, staging deque, block deque,
        # or leftover slot means the inputs had differing row counts.
        if any(self._awaiting_count) or self._pending_count_tasks:
            return

        has_buffered = any(buf.has_next() for buf in self._input_buffers)
        has_staged = any(len(s) > 0 for s in self._staging)
        has_remaining = any(len(d) > 0 for d in self._block_deques)
        has_leftover = any(leftover is not None for leftover in self._leftovers)
        if has_buffered or has_staged or has_remaining or has_leftover:
            # TODO(Clark): Support different number of rows via user-directed
            # dropping/padding instead of erroring out.
            raise ValueError("Cannot zip datasets of different number of rows")

    def has_next(self) -> bool:
        return self._output_buffer.has_next()

    def _get_next_inner(self) -> RefBundle:
        refs = self._output_buffer.get_next()
        self._metrics.on_output_dequeued(refs)
        self._output_blocks_stats.extend(to_stats(refs.metadata))
        return refs

    def get_active_tasks(self) -> List[OpTask]:
        return list(self._data_tasks.values()) + list(
            self._pending_count_tasks.values()
        )

    def num_active_tasks(self) -> int:
        return len(self._data_tasks) + len(self._pending_count_tasks)

    def get_stats(self) -> StatsDict:
        return {self._name: self._output_blocks_stats}

    def throttling_disabled(self) -> bool:
        return False

    def _fill_block_deque(self, input_index: int) -> None:
        """
        Stage ready (block_ref, num_rows) entries for the given input.

        If a block's metadata lacks a row count, a row-count task is submitted asynchronously
        and staging for this input pauses (to preserve order) until it resolves.
        """
        if self._awaiting_count[input_index]:
            return

        # Refill staging from input bundles until we have something to process
        # or the buffer is exhausted (skips any bundles that contribute no
        # blocks, so an empty bundle can't stall the input).
        while (
            not self._staging[input_index]
            and self._input_buffers[input_index].has_next()
        ):
            bundle = self._input_buffers[input_index].get_next()
            self._metrics.on_input_dequeued(bundle, input_index=input_index)
            self._staging[input_index].extend(bundle.blocks)

        # Move staged entries with a known row count into the block deque,
        # pausing at the first entry whose row count must be fetched.
        while self._staging[input_index]:
            entry = self._staging[input_index][0]
            num_rows = entry.metadata.num_rows
            if num_rows is None:
                self._submit_count_task(input_index, entry.ref)
                return
            self._staging[input_index].popleft()
            self._block_deques[input_index].append((entry.ref, num_rows))

    def _submit_count_task(self, input_index: int, block_ref: ray.ObjectRef) -> None:
        """
        Asynchronously fetch a block's row count without blocking the loop.
        """
        self._awaiting_count[input_index] = True

        label_selector = self.data_context.execution_options.label_selector
        count_fn = cached_remote_fn(_get_num_rows)
        if label_selector:
            count_fn = count_fn.options(label_selector=label_selector)
        count_ref = count_fn.remote(block_ref)

        task_index = self._next_meta_task_idx
        self._next_meta_task_idx += 1

        def _on_count_ready() -> None:
            self._pending_count_tasks.pop(task_index, None)
            self._awaiting_count[input_index] = False
            # The count object is ready (the executor only fires this after the
            # task completes), so this get is a local, non-blocking fetch.
            num_rows = ray.get(count_ref)
            entry = self._staging[input_index].popleft()
            self._block_deques[input_index].append((entry.ref, num_rows))
            # Resume making progress now that the row count is known.
            self._dispatch_ready_zips()
            self._validate_if_settled()

        self._pending_count_tasks[task_index] = MetadataOpTask(
            task_index, count_ref, _on_count_ready
        )

    def _has_data(self, input_index: int) -> bool:
        """Check if an input has data available (leftover, deque, or buffer)."""
        if self._leftovers[input_index] is not None:
            return True
        if len(self._block_deques[input_index]) > 0:
            return True
        self._fill_block_deque(input_index)
        return len(self._block_deques[input_index]) > 0

    def _has_data_from_all_inputs(self) -> bool:
        # Evaluate every input (no short-circuit) so a pending row-count fetch is
        # kicked off for each input that needs one, letting them run in parallel.
        results = [self._has_data(i) for i in range(len(self._input_buffers))]
        return all(results)

    def _pop_next_block(self, input_index: int) -> Tuple[ray.ObjectRef, int]:
        """Get the next block from an input, checking the leftover slot first."""
        if self._leftovers[input_index] is not None:
            ref, num_rows = self._leftovers[input_index]
            self._leftovers[input_index] = None
            return ref, num_rows
        if not self._block_deques[input_index]:
            self._fill_block_deque(input_index)
        return self._block_deques[input_index].popleft()

    def _dispatch_ready_zips(self) -> None:
        """
        Submit zip tasks for every set of blocks ready across all inputs.

        While a block is available from every input, takes one block from each,
        aligns them to the minimum row count (splitting larger blocks and storing
        the remainder as leftovers), and submits an asynchronous zip task for the
        aligned blocks. Repeats until some input has no data available.
        """
        label_selector = self.data_context.execution_options.label_selector

        split_fn = cached_remote_fn(_split_block_at_row, num_returns=2)
        if label_selector:
            split_fn = split_fn.options(label_selector=label_selector)

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

            self._submit_zip_task(aligned_refs)

    def _submit_zip_task(self, aligned_refs: List[ray.ObjectRef]) -> None:
        """Submit an asynchronous task that zips the aligned blocks together."""
        # TODO(ekl): Wire up per-task metrics so the progress bar and
        # task counters reflect zip tasks.
        label_selector = self.data_context.execution_options.label_selector
        zip_fn = cached_remote_fn(_zip_blocks_task, num_returns="streaming")
        if label_selector:
            zip_fn = zip_fn.options(label_selector=label_selector)

        task_index = self._next_task_idx
        self._next_task_idx += 1

        gen = zip_fn.remote(*aligned_refs)

        def _output_ready_callback(output: RefBundle) -> None:
            # The zip task streams exactly one output block.
            assert len(output) == 1
            self._output_buffer.add(output, key=task_index)
            self._metrics.on_output_queued(output)

        def _task_done_callback(
            exception: Optional[Exception],
            task_exec_stats=None,
            task_exec_driver_stats=None,
        ) -> None:
            self._data_tasks.pop(task_index, None)
            # Mark this ordering key complete so the output queue can advance.
            self._output_buffer.finalize(key=task_index)

        self._data_tasks[task_index] = DataOpTask(
            task_index,
            gen,
            _output_ready_callback,
            _task_done_callback,
            operator_name=self.name,
        )


def _split_block_at_row(block: Block, row_index: int) -> Tuple[Block, Block]:
    """Split a block into head ``[0, row_index)`` and tail ``[row_index, end)``."""
    accessor = BlockAccessor.for_block(block)
    head = accessor.slice(0, row_index)
    tail = accessor.slice(row_index, accessor.num_rows())
    return head, tail


def _zip_blocks_task(
    *blocks: Block,
) -> Iterator[Union[Block, bytes]]:
    """Streaming task that zips ``blocks`` column-wise and yields the result.

    Yields the zipped block followed by its pickled ``BlockMetadataWithSchema``,
    per the streaming-generator protocol expected by :class:`DataOpTask`.
    """
    stats = BlockExecStats.builder()
    # TODO(Clark): Extend BlockAccessor.zip() to accept N other blocks so we can
    # zip in a single call instead of folding pairwise.
    result = blocks[0]
    for other_block in blocks[1:]:
        result = BlockAccessor.for_block(result).zip(other_block)
    stats.finish()

    from ray.data.block import BlockMetadataWithSchema

    def build_metadata(block_ser_time_s: Optional[float]) -> "BlockMetadataWithSchema":
        return BlockMetadataWithSchema.from_block(
            result, block_exec_stats=stats.build(block_ser_time_s=block_ser_time_s)
        )

    yield from yield_block_with_stats(result, build_metadata)


def _get_num_rows(block: Block) -> int:
    return BlockAccessor.for_block(block).num_rows()

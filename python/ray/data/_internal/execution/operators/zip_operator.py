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
        # Bundles received from each input, before being staged into block deques.
        self._input_buffers: List[FIFOBundleQueue] = [
            FIFOBundleQueue() for _ in range(n)
        ]
        # Output blocks are emitted in submission order using the zip-task index
        # as the ordering key, since tasks may finish out of order.
        self._output_buffer: ReorderingBundleQueue = ReorderingBundleQueue()
        # Per-input raw blocks pulled from a bundle but not yet assigned a known
        # row count (entries are resolved in order; see ``_fill_block_deque``).
        self._staging: List[collections.deque] = [collections.deque() for _ in range(n)]
        # Per-input staged blocks as (block_ref, num_rows) awaiting alignment.
        self._block_deques: List[collections.deque] = [
            collections.deque() for _ in range(n)
        ]
        # Per-input remainder left over after a block was split for alignment.
        self._leftovers: List[Optional[Tuple[ray.ObjectRef, int]]] = [None] * n
        # Per-input flag: a row-count task is in flight for the head staged block,
        # so staging for that input is paused to preserve order.
        self._awaiting_count: List[bool] = [False] * n
        # In-flight zip tasks, keyed by task index. Keys index the output queue,
        # so they must stay a contiguous range.
        self._data_tasks: Dict[int, DataOpTask] = {}
        self._next_task_idx: int = 0
        # In-flight row-count tasks (used only when a block's metadata lacks a
        # row count). Tracked separately so their indices don't perturb the
        # contiguous output-queue keys above.
        self._pending_count_tasks: Dict[int, MetadataOpTask] = {}
        self._next_meta_task_idx: int = 0
        # Set once the executor has delivered every input bundle.
        self._inputs_fully_delivered: bool = False
        # Accumulated output stats, extended as outputs are taken.
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
        # Row-count validation can only run once every input is fully delivered
        # *and* nothing is still in flight. If row-count tasks are pending, the
        # check is deferred to the last one's completion callback instead.
        self._validate_if_settled()
        super().all_inputs_done()

    def _validate_if_settled(self) -> None:
        """Raise if inputs have unequal row counts, once everything has settled.

        "Settled" means all inputs are delivered, no row-count tasks are in
        flight, and nothing remains staged or buffered. If blocks are still left
        over at that point, the inputs had differing row counts.
        """
        if not self._inputs_fully_delivered:
            return
        if any(self._awaiting_count) or self._pending_count_tasks:
            return
        if any(len(s) > 0 for s in self._staging):
            return
        if any(buf.has_next() for buf in self._input_buffers):
            return

        has_leftover = any(leftover is not None for leftover in self._leftovers)
        has_remaining = any(len(d) > 0 for d in self._block_deques)
        if has_leftover or has_remaining:
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
        """Stage ready (block_ref, num_rows) entries for the given input.

        Pulls blocks from the next input bundle into a per-input staging deque
        and moves those with a known row count into the block deque. If a block's
        metadata lacks a row count, a row-count task is submitted asynchronously
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
        """Asynchronously fetch a block's row count without blocking the loop.

        Used only when a block's metadata lacks ``num_rows`` (rare; some external
        datasources don't populate it). The block stays at the head of the input's
        staging deque until the count resolves.
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
        """Submit zip tasks for every set of blocks ready across all inputs.

        While a block is available from every input, takes one block from each,
        aligns them to the minimum row count (splitting larger blocks and storing
        the remainder as leftovers), and submits an asynchronous zip task for the
        aligned blocks. Repeats until some input has no data available.

        Alignment decisions use the row counts already known from block metadata,
        so this method does not block waiting on remote results.
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

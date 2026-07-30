import collections
from dataclasses import dataclass
from typing import TYPE_CHECKING, Deque, Dict, Iterator, List, Optional, Tuple, Union

from typing_extensions import override

import ray
from ray.data._internal.execution.bundle_queue import (
    BaseBundleQueue,
    FIFOBundleQueue,
    ReorderingBundleQueue,
)
from ray.data._internal.execution.interfaces import (
    ExecutionOptions,
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

    from ray.data._internal.execution.block_ref_counter import BlockRefCounter
    from ray.data.block import BlockMetadataWithSchema


@dataclass
class _SourceBundle:
    """An input bundle whose blocks the operator is still holding.

    A bundle's blocks stay live while any of its rows are unzipped *or* any zip
    task still references them, which outlives the bundle leaving the input
    queue. ``holds`` tracks both so the bundle's memory keeps counting against
    this operator (and stays visible to backpressure) for exactly as long as it
    is really held, and is released and freed as soon as it isn't.
    """

    bundle: RefBundle
    input_index: int
    # Unconsumed slices, plus in-flight zip tasks referencing this bundle.
    holds: int


@dataclass
class _BlockSlice:
    """A view of the rows ``[offset, offset + num_rows)`` of a block.

    Zipping consumes a slice from the front as rows are paired off, which is
    done by advancing ``offset`` rather than materializing a new block. The
    zip task applies the slice locally, so no intermediate objects are ever
    written to the object store.
    """

    ref: ray.ObjectRef
    offset: int
    # Rows remaining in this slice, or ``None`` until the block's row count has
    # been resolved (see ``ZipOperator._submit_count_task``).
    num_rows: Optional[int]
    # The input bundle this block came from, released once nothing holds it.
    source: _SourceBundle


class ZipOperator(InternalQueueOperatorMixin, NAryOperator):
    """An operator that zips its inputs together in a streaming fashion.

    Blocks are processed incrementally as they arrive from all inputs. Whenever
    a block is available from every input, the operator zips the longest row
    range they have in common and advances each input past it, carrying the
    unconsumed remainder forward as an offset into the same block.

    All remote work is submitted as :class:`OpTask`s and surfaced through
    :meth:`get_active_tasks`, so the streaming executor drives them without ever
    blocking its scheduling loop on ``ray.get``.
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
        # Per-input queue of not-yet-zipped row ranges, in order. The head is the
        # next range to pair off; a partially consumed head simply carries a
        # non-zero offset.
        self._pending: List[Deque[_BlockSlice]] = [
            collections.deque() for _ in range(n)
        ]
        # In-flight row-count fetches, keyed by input index. An input with an
        # entry here is paused until its head's row count resolves.
        self._pending_count_tasks: Dict[int, MetadataOpTask] = {}
        # The bundle each in-flight row-count fetch is holding, keyed by input
        # index. Tracked like `_task_sources` so a cancelled fetch still
        # releases what it held.
        self._count_task_sources: Dict[int, _SourceBundle] = {}
        # In-flight zip tasks, keyed by task index (also the output ordering key).
        self._data_tasks: Dict[int, DataOpTask] = {}
        # The bundles each in-flight zip task is holding, keyed by task index.
        # Tracked outside the task's callback so the holds can still be released
        # if the task is cancelled and the callback never runs.
        self._task_sources: Dict[int, List[_SourceBundle]] = {}
        self._next_task_idx: int = 0
        # Replaced in `start()` once the ordering requirement is known.
        self._output_buffer: BaseBundleQueue = FIFOBundleQueue()
        self._inputs_fully_delivered: bool = False
        self._output_blocks_stats: List[BlockStats] = []
        self._stats: StatsDict = {}
        super().__init__(
            data_context,
            *input_ops,
        )

    def start(
        self,
        options: ExecutionOptions,
        block_ref_counter: "BlockRefCounter",
    ) -> None:
        super().start(options, block_ref_counter)
        # Zip tasks can complete out of order, so only pay for reordering when
        # the execution actually requires the input order to be preserved.
        if options.preserve_order:
            self._output_buffer = ReorderingBundleQueue()
        else:
            self._output_buffer = FIFOBundleQueue()

    @property
    @override
    def _input_queues(self) -> List["BaseBundleQueue"]:
        return self._input_buffers

    @property
    @override
    def _output_queues(self) -> List["BaseBundleQueue"]:
        return [self._output_buffer]

    @property
    def _num_inputs(self) -> int:
        return len(self._input_buffers)

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
        self._validate_if_settled()
        super().all_inputs_done()

    def _validate_if_settled(self) -> None:
        """Raise if the inputs turned out to have different numbers of rows.

        Only meaningful once every input has been delivered and no row count is
        still being fetched; until then the check is deferred (a pending count
        task re-runs it from its completion callback).
        """
        if not self._inputs_fully_delivered or self._pending_count_tasks:
            return

        # NOTE: Evaluate every input (rather than short-circuiting) so each one
        # gets the chance to drop trailing empty blocks and, if needed, start its
        # row-count fetch.
        has_rows = [self._ensure_head(i) for i in range(self._num_inputs)]
        if self._pending_count_tasks:
            # A count fetch was just started; its callback re-runs this check.
            return
        if any(has_rows):
            # Inputs are fully delivered and nothing is in flight, so every row
            # that could be paired has been. Rows left over on any input mean the
            # inputs were of different lengths.
            #
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

    def _ensure_head(self, input_index: int) -> bool:
        """Make the input's queue head a non-empty slice with a known row count.

        Pulls in more blocks and drops empty ones as needed.

        Args:
            input_index: Index of the input to advance.

        Returns:
            Whether the input has rows ready to zip. ``False`` means the input is
            exhausted, or its head's row count is still being fetched (in which
            case a task is in flight and progress resumes from its callback).
        """
        if input_index in self._pending_count_tasks:
            return False

        pending = self._pending[input_index]
        while True:
            # Pull in the next bundle's blocks, skipping bundles that carry none
            # (an empty bundle must not stall the input).
            while not pending and self._input_buffers[input_index].has_next():
                bundle = self._input_buffers[input_index].get_next()
                # NOTE: The bundle stays accounted for as queued input until its
                # last hold is released; see `_release_hold`.
                source = _SourceBundle(bundle, input_index, len(bundle.blocks))
                if not source.holds:
                    self._release_bundle(source)
                    continue
                pending.extend(
                    _BlockSlice(entry.ref, 0, entry.metadata.num_rows, source)
                    for entry in bundle.blocks
                )
            if not pending:
                return False

            head = pending[0]
            if head.num_rows is None:
                # Alignment needs the row count on the driver, so fetch it
                # asynchronously and pause this input until it arrives.
                self._submit_count_task(input_index, head)
                return False
            if head.num_rows == 0:
                # Empty blocks contribute no rows, so drop them instead of
                # pairing them off; otherwise they'd linger as phantom leftovers
                # and trip the row-count validation above.
                pending.popleft()
                self._release_hold(head.source)
                continue
            return True

    def _do_shutdown(self, force: bool) -> None:
        super()._do_shutdown(force)
        # Cancelled tasks never run their completion callbacks, so release the
        # bundles they were holding and drop the tasks here. Rows still staged
        # for zipping are released separately, by `clear_internal_input_queue`.
        for task_index in list(self._task_sources):
            self._release_task_holds(task_index)
        for input_index in list(self._count_task_sources):
            self._release_count_hold(input_index)
        self._data_tasks.clear()
        self._pending_count_tasks.clear()

    def _release_task_holds(self, task_index: int) -> None:
        """Release the holds a zip task kept on the bundles it read."""
        for source in self._task_sources.pop(task_index, ()):
            self._release_hold(source)

    def _release_count_hold(self, input_index: int) -> None:
        """Release the hold a row-count fetch kept on the bundle it read."""
        source = self._count_task_sources.pop(input_index, None)
        if source is not None:
            self._release_hold(source)

    @override
    def clear_internal_input_queue(self) -> None:
        """Drop buffered input and stop accounting for it.

        Runs when execution ends before the inputs do, such as a downstream
        limit being reached, as well as on shutdown. Whatever the operator is
        still holding has to be released here, or those bundles stay counted in
        its input metrics and blocks it owns are never freed.
        """
        for input_index, buffer in enumerate(self._input_buffers):
            # Rows staged for zipping that never got consumed.
            pending = self._pending[input_index]
            while pending:
                self._release_hold(pending.popleft().source)
            # Bundles that never even reached the staging queue.
            while buffer.has_next():
                bundle = buffer.get_next()
                self._metrics.on_input_dequeued(bundle, input_index=input_index)
                bundle.destroy_if_owned()
        super().clear_internal_input_queue()

    def _release_hold(self, source: _SourceBundle) -> None:
        """Drop one hold on an input bundle, releasing it once none remain."""
        source.holds -= 1
        assert source.holds >= 0, source
        if source.holds == 0:
            self._release_bundle(source)

    def _release_bundle(self, source: _SourceBundle) -> None:
        """Stop accounting for an input bundle and free its blocks if owned.

        Called once nothing references the bundle: every row has been zipped and
        every task that read it has finished. Blocks shared with other operators
        (``owns_blocks=False``) aren't freed.
        """
        self._metrics.on_input_dequeued(source.bundle, input_index=source.input_index)
        source.bundle.destroy_if_owned()

    def _submit_count_task(self, input_index: int, head: _BlockSlice) -> None:
        """Asynchronously resolve ``head``'s row count without blocking the loop.

        Only needed for the rare block whose metadata carries no row count.
        """
        label_selector = self.data_context.execution_options.label_selector
        count_fn = cached_remote_fn(_get_num_rows)
        if label_selector:
            count_fn = count_fn.options(label_selector=label_selector)
        count_ref = count_fn.remote(head.ref)

        # The task reads the block, so hold its bundle for as long as the task
        # runs. Without this an early finish could free the block out from under
        # a count still in flight.
        head.source.holds += 1
        self._count_task_sources[input_index] = head.source

        def _on_count_ready() -> None:
            self._pending_count_tasks.pop(input_index, None)
            # The executor only fires this once the task has completed, so the
            # object is already available and this is a local fetch.
            head.num_rows = ray.get(count_ref)
            self._release_count_hold(input_index)
            self._dispatch_ready_zips()
            self._validate_if_settled()

        self._pending_count_tasks[input_index] = MetadataOpTask(
            input_index, count_ref, _on_count_ready
        )

    def _dispatch_ready_zips(self) -> None:
        """Submit a zip task for every row range the inputs currently share.

        Each round zips the longest prefix common to all inputs' heads and
        advances each head past it, leaving any remainder in place as an offset.
        """
        # NOTE: Build the list first (rather than short-circuiting inside `all`)
        # so every input can start its row-count fetch, letting them overlap.
        while all([self._ensure_head(i) for i in range(self._num_inputs)]):
            heads = [self._pending[i][0] for i in range(self._num_inputs)]
            num_rows = min(head.num_rows for head in heads)
            assert num_rows > 0, heads

            # The task reads these blocks, so hold their bundles until it ends.
            # A block can feed several tasks (at different offsets), so it must
            # not be freed when only the first of them finishes.
            sources = [head.source for head in heads]
            for source in sources:
                source.holds += 1

            self._submit_zip_task(
                [(head.ref, head.offset) for head in heads], num_rows, sources
            )

            for input_index, head in enumerate(heads):
                head.offset += num_rows
                head.num_rows -= num_rows
                if head.num_rows == 0:
                    self._pending[input_index].popleft()
                    self._release_hold(head.source)

    def _submit_zip_task(
        self,
        block_slices: List[Tuple[ray.ObjectRef, int]],
        num_rows: int,
        sources: List[_SourceBundle],
    ) -> None:
        """Submit a task zipping ``num_rows`` rows from each of ``block_slices``.

        Args:
            block_slices: One ``(block_ref, offset)`` per input, identifying where
                that input's contribution starts.
            num_rows: How many rows to take from each block, starting at its
                offset. Equal across inputs, so the rows line up.
            sources: The input bundles these blocks came from. Each holds one
                reference for this task, released when it finishes.
        """
        # TODO(ekl): Wire up per-task metrics (`on_task_submitted` and friends)
        # so the progress bar and task counters reflect zip tasks. NOTE: input
        # memory is already accounted for via `_SourceBundle`, which keeps each
        # bundle in this operator's input-queue metrics until nothing holds it.
        label_selector = self.data_context.execution_options.label_selector
        zip_fn = cached_remote_fn(_zip_blocks_task, num_returns="streaming")
        if label_selector:
            zip_fn = zip_fn.options(label_selector=label_selector)

        task_index = self._next_task_idx
        self._next_task_idx += 1
        self._task_sources[task_index] = sources

        gen = zip_fn.remote(
            *[ref for ref, _ in block_slices],
            offsets=[offset for _, offset in block_slices],
            num_rows=num_rows,
        )

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
            # The task is done reading its inputs, so they can now be freed
            # (unless another task or unzipped range still holds them).
            self._release_task_holds(task_index)

        self._data_tasks[task_index] = DataOpTask(
            task_index,
            gen,
            self._block_ref_counter,
            self.id,
            output_ready_callback=_output_ready_callback,
            task_done_callback=_task_done_callback,
            operator_name=self.name,
        )


def _zip_blocks_task(
    *blocks: Block,
    offsets: List[int],
    num_rows: int,
) -> Iterator[Union[Block, bytes]]:
    """Zip the aligned row range of ``blocks`` and yield the resulting block.

    Each block is sliced to ``[offset, offset + num_rows)`` locally before
    zipping, so misaligned block boundaries are handled without materializing
    intermediate split blocks in the object store.

    Yields the zipped block followed by its pickled ``BlockMetadataWithSchema``,
    per the streaming-generator protocol expected by :class:`DataOpTask`.
    """
    stats = BlockExecStats.builder()

    # TODO(Clark): Extend BlockAccessor.zip() to accept N other blocks so we can
    # zip in a single call instead of folding pairwise.
    result = None
    for block, offset in zip(blocks, offsets):
        accessor = BlockAccessor.for_block(block)
        if offset != 0 or accessor.num_rows() != num_rows:
            block = accessor.slice(offset, offset + num_rows)
        if result is None:
            result = block
        else:
            result = BlockAccessor.for_block(result).zip(block)
    stats.finish()

    from ray.data.block import BlockMetadataWithSchema

    def build_metadata(block_ser_time_s: Optional[float]) -> "BlockMetadataWithSchema":
        return BlockMetadataWithSchema.from_block(
            result, block_exec_stats=stats.build(block_ser_time_s=block_ser_time_s)
        )

    yield from yield_block_with_stats(result, build_metadata)


def _get_num_rows(block: Block) -> int:
    return BlockAccessor.for_block(block).num_rows()

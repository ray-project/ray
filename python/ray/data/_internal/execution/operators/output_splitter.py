import math
import time
from collections import deque
from typing import Any, Dict, List, Optional, Tuple

from ray.data._internal.execution.interfaces import (
    ExecutionOptions,
    NodeIdStr,
    PhysicalOperator,
    RefBundle,
)
from ray.data._internal.execution.util import locality_string
from ray.data._internal.remote_fn import cached_remote_fn
from ray.data._internal.stats import StatsDict
from ray.data.block import Block, BlockAccessor, BlockMetadata
from ray.data.context import DataContext
from ray.types import ObjectRef


class OutputSplitter(PhysicalOperator):
    """An operator that splits the given data into `n` output splits.

    The output bundles of this operator will have a `bundle.output_split_idx` attr
    set to an integer from [0..n-1]. This operator tries to divide the rows evenly
    across output splits. If the `equal` option is set, the operator will furthermore
    guarantee an exact split of rows across outputs, truncating the Dataset.

    Implementation wise, this operator keeps an internal buffer of bundles. The buffer
    has a minimum size calculated to enable a good locality hit rate, as well as ensure
    we can satisfy the `equal` requirement.

    OutputSplitter does not provide any ordering guarantees.
    """

    def __init__(
        self,
        input_op: PhysicalOperator,
        n: int,
        equal: bool,
        data_context: DataContext,
        locality_hints: Optional[List[NodeIdStr]] = None,
    ):
        super().__init__(
            f"split({n}, equal={equal})",
            [input_op],
            data_context,
            target_max_block_size=None,
        )
        self._equal = equal
        # Buffer of bundles not yet assigned to output splits.
        self._buffer: List[RefBundle] = []
        # The outputted bundles with output_split attribute set.
        self._output_queue: deque[RefBundle] = deque()
        # The number of rows output to each output split so far.
        self._num_output: List[int] = [0 for _ in range(n)]
        # The time of the overhead for the output splitter (operator level)
        self._output_splitter_overhead_time = 0

        if locality_hints is not None:
            if n != len(locality_hints):
                raise ValueError(
                    "Locality hints list must have length `n`: "
                    f"len({locality_hints}) != {n}"
                )
        self._locality_hints = locality_hints
        if locality_hints:
            # To optimize locality, we should buffer a certain number of elements
            # internally before dispatch to allow the locality algorithm a good chance
            # of selecting a preferred location. We use a small multiple of `n` since
            # it's reasonable to buffer a couple blocks per consumer.
            self._min_buffer_size = 2 * n
        else:
            self._min_buffer_size = 0
        self._locality_hits = 0
        self._locality_misses = 0

    def num_outputs_total(self) -> Optional[int]:
        # OutputSplitter does not change the number of blocks,
        # so we can return the number of blocks from the input op.
        return self.input_dependencies[0].num_outputs_total()

    def num_output_rows_total(self) -> Optional[int]:
        # The total number of rows is the same as the number of input rows.
        return self.input_dependencies[0].num_output_rows_total()

    def start(self, options: ExecutionOptions) -> None:
        super().start(options)
        # Force disable locality optimization.
        if not options.actor_locality_enabled:
            self._locality_hints = None
            self._min_buffer_size = 0

    def throttling_disabled(self) -> bool:
        """Disables resource-based throttling.

        It doesn't make sense to throttle the inputs to this operator, since all that
        would do is lower the buffer size and prevent us from emitting outputs /
        reduce the locality hit rate.
        """
        return True

    def has_next(self) -> bool:
        return len(self._output_queue) > 0

    def _get_next_inner(self) -> RefBundle:
        output = self._output_queue.popleft()
        self._metrics.on_output_dequeued(output)
        return output

    def get_stats(self) -> StatsDict:
        return {"split": []}  # TODO(ekl) add split metrics?

    def _extra_metrics(self) -> Dict[str, Any]:
        stats = {}
        for i, num in enumerate(self._num_output):
            stats[f"num_output_{i}"] = num
        stats["output_splitter_overhead_time"] = self._output_splitter_overhead_time
        return stats

    def _add_input_inner(self, bundle, input_index) -> None:
        if bundle.num_rows() is None:
            raise ValueError("OutputSplitter requires bundles with known row count")
        self._buffer.append(bundle)
        self._metrics.on_input_queued(bundle)
        self._dispatch_bundles()

    def all_inputs_done(self) -> None:
        super().all_inputs_done()
        if not self._equal:
            self._dispatch_bundles(dispatch_all=True)
            assert not self._buffer, "Should have dispatched all bundles."
            return

        # Otherwise:
        # Need to finalize distribution of buffered data to output splits.
        buffer_size = sum(b.num_rows() for b in self._buffer)
        max_n = max(self._num_output)

        # First calculate the min rows to add per output to equalize them.
        allocation = [max_n - n for n in self._num_output]
        remainder = buffer_size - sum(allocation)
        # Invariant: buffer should always be large enough to equalize.
        assert remainder >= 0, (remainder, buffer_size, allocation)

        # Equally distribute remaining rows in buffer to outputs.
        x = remainder // len(allocation)
        allocation = [a + x for a in allocation]

        # Execute the split.
        for i, count in enumerate(allocation):
            bundles = self._split_from_buffer(count)
            for b in bundles:
                b.output_split_idx = i
                self._output_queue.append(b)
                self._metrics.on_output_queued(b)
        self._buffer = []

    def internal_queue_size(self) -> int:
        return len(self._buffer)

    def progress_str(self) -> str:
        if self._locality_hints:
            return locality_string(self._locality_hits, self._locality_misses)
        else:
            return "[locality disabled]"

    def _dispatch_bundles(self, dispatch_all: bool = False) -> None:
        start_time = time.perf_counter()
        # Dispatch all dispatchable bundles from the internal buffer.
        # This may not dispatch all bundles when equal=True.
        while self._buffer and (
            dispatch_all or len(self._buffer) >= self._min_buffer_size
        ):
            target_index = self._select_output_index()
            target_bundle = self._pop_bundle_to_dispatch(target_index)
            if self._can_safely_dispatch(target_index, target_bundle.num_rows()):
                target_bundle.output_split_idx = target_index
                self._num_output[target_index] += target_bundle.num_rows()
                self._output_queue.append(target_bundle)
                self._metrics.on_output_queued(target_bundle)
                if self._locality_hints:
                    preferred_loc = self._locality_hints[target_index]
                    if self._get_location(target_bundle) == preferred_loc:
                        self._locality_hits += 1
                    else:
                        self._locality_misses += 1
            else:
                # Put it back and abort.
                self._buffer.insert(0, target_bundle)
                self._metrics.on_input_queued(target_bundle)
                break
        self._output_splitter_overhead_time += time.perf_counter() - start_time

    def _select_output_index(self) -> int:
        # Greedily dispatch to the consumer with the least data so far.
        i, _ = min(enumerate(self._num_output), key=lambda t: t[1])
        return i

    def _pop_bundle_to_dispatch(self, target_index: int) -> RefBundle:
        if self._locality_hints:
            preferred_loc = self._locality_hints[target_index]
            for bundle in self._buffer:
                if self._get_location(bundle) == preferred_loc:
                    self._buffer.remove(bundle)
                    self._metrics.on_input_dequeued(bundle)
                    return bundle

        bundle = self._buffer.pop(0)
        self._metrics.on_input_dequeued(bundle)
        return bundle

    def _can_safely_dispatch(self, target_index: int, nrow: int) -> bool:
        if not self._equal:
            # If not in equals mode, dispatch away with no buffer requirements.
            return True
        output_distribution = self._num_output.copy()
        output_distribution[target_index] += nrow
        buffer_requirement = self._calculate_buffer_requirement(output_distribution)
        buffer_size = sum(b.num_rows() for b in self._buffer)
        return buffer_size >= buffer_requirement

    def _calculate_buffer_requirement(self, output_distribution: List[int]) -> int:
        # Calculate the new number of rows that we'd need to equalize the row
        # distribution after the bundle dispatch.
        max_n = max(output_distribution)
        return sum([max_n - n for n in output_distribution])

    def _split_from_buffer(self, nrow: int) -> List[RefBundle]:
        output = []
        acc = 0
        while acc < nrow:
            b = self._buffer.pop()
            self._metrics.on_input_dequeued(b)
            if acc + b.num_rows() <= nrow:
                output.append(b)
                acc += b.num_rows()
            else:
                left, right = _split(b, nrow - acc)
                output.append(left)
                acc += left.num_rows()
                self._buffer.append(right)
                self._metrics.on_input_queued(right)
                assert acc == nrow, (acc, nrow)

        assert sum(b.num_rows() for b in output) == nrow, (acc, nrow)
        return output

    def _get_location(self, bundle: RefBundle) -> Optional[NodeIdStr]:
        """Ask Ray for the node id of the given bundle.

        This method may be overriden for testing.

        Returns:
            A node id associated with the bundle, or None if unknown.
        """
        return bundle.get_cached_location()

    def implements_accurate_memory_accounting(self) -> bool:
        return True


def _split(bundle: RefBundle, left_size: int) -> Tuple[RefBundle, RefBundle]:
    left_blocks, left_meta = [], []
    right_blocks, right_meta = [], []
    acc = 0
    for b, m in bundle.blocks:
        if acc >= left_size:
            right_blocks.append(b)
            right_meta.append(m)
        elif acc + m.num_rows <= left_size:
            left_blocks.append(b)
            left_meta.append(m)
            acc += m.num_rows
        else:
            # Trouble case: split it up.
            lm, rm = _split_meta(m, left_size - acc)
            lb, rb = _split_block(b, left_size - acc)
            left_meta.append(lm)
            right_meta.append(rm)
            left_blocks.append(lb)
            right_blocks.append(rb)
            acc += lm.num_rows
            assert acc == left_size
    left = RefBundle(list(zip(left_blocks, left_meta)), owns_blocks=bundle.owns_blocks)
    right = RefBundle(
        list(zip(right_blocks, right_meta)), owns_blocks=bundle.owns_blocks
    )
    assert left.num_rows() == left_size
    assert left.num_rows() + right.num_rows() == bundle.num_rows()
    return left, right


def _split_meta(
    m: BlockMetadata, left_size: int
) -> Tuple[BlockMetadata, BlockMetadata]:
    left_bytes = int(math.floor(m.size_bytes * (left_size / m.num_rows)))
    left = BlockMetadata(
        num_rows=left_size,
        size_bytes=left_bytes,
        schema=m.schema,
        input_files=m.input_files,
        exec_stats=None,
    )
    right = BlockMetadata(
        num_rows=m.num_rows - left_size,
        size_bytes=m.size_bytes - left_bytes,
        schema=m.schema,
        input_files=m.input_files,
        exec_stats=None,
    )
    return left, right


def _split_block(
    b: ObjectRef[Block], left_size: int
) -> Tuple[ObjectRef[Block], ObjectRef[Block]]:
    split_single_block = cached_remote_fn(_split_single_block)
    left, right = split_single_block.options(num_cpus=0, num_returns=2).remote(
        b, left_size
    )
    return left, right


def _split_single_block(b: Block, left_size: int) -> Tuple[Block, Block]:
    acc = BlockAccessor.for_block(b)
    left = acc.slice(0, left_size)
    right = acc.slice(left_size, acc.num_rows())
    assert BlockAccessor.for_block(left).num_rows() == left_size
    assert BlockAccessor.for_block(right).num_rows() == (acc.num_rows() - left_size)
    return left, right

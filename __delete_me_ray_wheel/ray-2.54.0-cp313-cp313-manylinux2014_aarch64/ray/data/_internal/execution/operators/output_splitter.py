import math
import time
from typing import Any, Collection, Dict, List, Optional, Tuple

from ray._private.ray_constants import env_float
from ray.data._internal.execution.bundle_queue import (
    FIFOBundleQueue,
    HashLinkedQueue,
)
from ray.data._internal.execution.interfaces import (
    ExecutionOptions,
    NodeIdStr,
    PhysicalOperator,
    RefBundle,
)
from ray.data._internal.execution.operators.base_physical_operator import (
    InternalQueueOperatorMixin,
)
from ray.data._internal.execution.util import locality_string
from ray.data._internal.remote_fn import cached_remote_fn
from ray.data._internal.stats import StatsDict
from ray.data.block import Block, BlockAccessor, BlockMetadata
from ray.data.context import DataContext
from ray.types import ObjectRef

DEFAULT_OUTPUT_SPLITTER_MAX_BUFFERING_FACTOR = env_float(
    "RAY_DATA_DEFAULT_OUTPUT_SPLITTER_MAX_BUFFERING_FACTOR", 2
)


class OutputSplitter(InternalQueueOperatorMixin, PhysicalOperator):
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
        )
        self._equal = equal
        # Buffer of bundles not yet assigned to output splits.
        self._buffer: HashLinkedQueue = HashLinkedQueue()
        # The outputted bundles with output_split attribute set.
        self._output_queue: FIFOBundleQueue = FIFOBundleQueue()
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

        # To optimize locality, we might defer dispatching of the bundles to allow
        # for better node affinity by allowing next receiver to wait for a block
        # with preferred locality (minimizing data movement).
        #
        # However, to guarantee liveness we cap buffering to not exceed
        #
        #   DEFAULT_OUTPUT_SPLITTER_MAX_BUFFERING_FACTOR * N
        #
        # Where N is the number of outputs the sequence is being split into
        if locality_hints:
            self._max_buffer_size = DEFAULT_OUTPUT_SPLITTER_MAX_BUFFERING_FACTOR * n
        else:
            self._max_buffer_size = 0

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
        if options.preserve_order:
            # If preserve_order is set, we need to ignore locality hints to ensure determinism.
            self._locality_hints = None
            self._max_buffer_size = 0

        super().start(options)

    def throttling_disabled(self) -> bool:
        """Disables resource-based throttling.

        It doesn't make sense to throttle the inputs to this operator, since all that
        would do is lower the buffer size and prevent us from emitting outputs /
        reduce the locality hit rate.
        """
        return True

    def has_next(self) -> bool:
        return self._output_queue.has_next()

    def _get_next_inner(self) -> RefBundle:
        output = self._output_queue.get_next()
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
        self._buffer.add(bundle)
        self._metrics.on_input_queued(bundle)
        # Try dispatch buffered bundles
        self._try_dispatch_bundles()

    def all_inputs_done(self) -> None:
        super().all_inputs_done()

        # First, attempt to dispatch bundles based on the locality preferences
        # (if configured)
        if self._locality_hints:
            # NOTE: If equal distribution is not requested, we will force
            #       the dispatching
            self._try_dispatch_bundles(force=not self._equal)

            if not self._equal:
                assert not self._buffer, "All bundles should have been dispatched"
                return

        if not self._buffer:
            return

        # Otherwise:
        # Need to finalize distribution of buffered data to output splits.
        buffer_size = self._buffer.num_rows()
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
                self._output_queue.add(b)
                self._metrics.on_output_queued(b)
        self._buffer.clear()

    def internal_input_queue_num_blocks(self) -> int:
        return self._buffer.num_blocks()

    def internal_input_queue_num_bytes(self) -> int:
        return self._buffer.estimate_size_bytes()

    def internal_output_queue_num_blocks(self) -> int:
        return self._output_queue.num_blocks()

    def internal_output_queue_num_bytes(self) -> int:
        return self._output_queue.estimate_size_bytes()

    def clear_internal_input_queue(self) -> None:
        """Clear internal input queue."""
        while self._buffer:
            bundle = self._buffer.get_next()
            self._metrics.on_input_dequeued(bundle)

    def clear_internal_output_queue(self) -> None:
        """Clear internal output queue."""
        while self._output_queue.has_next():
            bundle = self._output_queue.get_next()
            self._metrics.on_output_dequeued(bundle)

    def progress_str(self) -> str:
        if self._locality_hints:
            return locality_string(self._locality_hits, self._locality_misses)
        else:
            return "[locality disabled]"

    def _try_dispatch_bundles(self, force: bool = False) -> None:
        start_time = time.perf_counter()

        # Currently, there are 2 modes of operation when dispatching
        # accumulated bundles:
        #
        #   1. Best-effort: we do a single pass over the whole buffer
        #      and try to dispatch all bundles either
        #
        #      a) Based on their locality (if feasible)
        #      b) Longest-waiting if buffer exceeds max-size threshold
        #
        #   2. Mandatory: when whole buffer has to be dispatched (for ex,
        #      upon completion of the dataset execution)
        #
        for _ in range(len(self._buffer)):
            # Get target output index of the next receiver
            target_output_index = self._select_next_output_index()
            # Look up preferred bundle
            preferred_bundle = self._find_preferred_bundle(target_output_index)

            if preferred_bundle:
                target_bundle = preferred_bundle
            elif len(self._buffer) >= self._max_buffer_size or force:
                # If we're not able to find a preferred bundle and buffer size is above
                # the cap, we pop the longest awaiting and pass to the next receiver
                target_bundle = self._buffer.peek_next()
                assert target_bundle is not None
            else:
                # Provided that we weren't able to either locate preferred bundle
                # or dequeue the head one, we bail out from iteration
                break

            # In case, when we can't safely dispatch (to avoid violating distribution
            # requirements), short-circuit
            if not self._can_safely_dispatch(
                target_output_index, target_bundle.num_rows()
            ):
                break

            # Pop preferred bundle from the buffer
            self._buffer.remove(target_bundle)
            self._metrics.on_input_dequeued(target_bundle)

            target_bundle.output_split_idx = target_output_index

            self._num_output[target_output_index] += target_bundle.num_rows()
            self._output_queue.add(target_bundle)
            self._metrics.on_output_queued(target_bundle)

            if self._locality_hints:
                if preferred_bundle:
                    self._locality_hits += 1
                else:
                    self._locality_misses += 1

        self._output_splitter_overhead_time += time.perf_counter() - start_time

    def _select_next_output_index(self) -> int:
        # Greedily dispatch to the consumer with the least data so far.
        i, _ = min(enumerate(self._num_output), key=lambda t: t[1])
        return i

    def _find_preferred_bundle(self, target_output_index: int) -> Optional[RefBundle]:
        if self._locality_hints:
            preferred_loc = self._locality_hints[target_output_index]

            # TODO make this more efficient (adding inverse hash-map)
            for bundle in self._buffer:
                if preferred_loc in self._get_locations(bundle):
                    return bundle

        return None

    def _can_safely_dispatch(self, target_index: int, target_num_rows: int) -> bool:
        if not self._equal:
            # If not in equals mode, dispatch away with no buffer requirements.
            return True

        # Simulate dispatching a bundle to the target receiver
        output_distribution = self._num_output.copy()
        output_distribution[target_index] += target_num_rows
        buffer_requirement = self._calculate_buffer_requirement(output_distribution)
        # Subtract target bundle size from the projected buffer
        buffer_size = self._buffer.num_rows() - target_num_rows
        # Check if we have enough rows LEFT after dispatching to equalize.
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
            b = self._buffer.get_next()
            self._metrics.on_input_dequeued(b)
            if acc + b.num_rows() <= nrow:
                output.append(b)
                acc += b.num_rows()
            else:
                left, right = _split(b, nrow - acc)
                output.append(left)
                acc += left.num_rows()
                self._buffer.add(right)
                self._metrics.on_input_queued(right)
                assert acc == nrow, (acc, nrow)

        assert sum(b.num_rows() for b in output) == nrow, (acc, nrow)
        return output

    @staticmethod
    def _get_locations(bundle: RefBundle) -> Collection[NodeIdStr]:
        """Fetches list of node ids holding the objects of the given bundle.

        This method may be overridden for testing.

        Returns:
            A list of node ids where the objects in the bundle are located
        """
        preferred_locations = bundle.get_preferred_object_locations()

        return preferred_locations.keys()


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

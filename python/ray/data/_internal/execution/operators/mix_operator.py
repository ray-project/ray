from typing import List, Optional

from ray.data._internal.execution.bundle_queue import FIFOBundleQueue
from ray.data._internal.execution.interfaces import (
    PhysicalOperator,
    RefBundle,
)
from ray.data._internal.execution.operators.base_physical_operator import (
    InternalQueueOperatorMixin,
    NAryOperator,
)
from ray.data._internal.stats import StatsDict
from ray.data._internal.stopping_condition import StoppingCondition
from ray.data.context import DataContext


class MixOperator(InternalQueueOperatorMixin, NAryOperator):
    """An operator that interleaves blocks from multiple input operators
    into a single output stream, respecting target row ratios specified
    by weights.

    Uses a deficit-adjusted algorithm: tracks cumulative row counts per
    input and always pulls from whichever input has fallen furthest behind
    its target ratio. This ensures the output row ratio converges to the
    target weights regardless of input block size variance.
    """

    def __init__(
        self,
        data_context: DataContext,
        *input_ops: PhysicalOperator,
        weights: List[float],
        stopping_condition: StoppingCondition = StoppingCondition.STOP_ON_SHORTEST,
    ):
        assert len(input_ops) >= 1
        assert len(weights) == len(input_ops)

        # Normalize weights to sum to 1.
        total_weight = sum(weights)
        self._weights = [w / total_weight for w in weights]

        self._stopping_condition = stopping_condition

        self._input_buffers: List[FIFOBundleQueue] = [
            FIFOBundleQueue() for _ in range(len(input_ops))
        ]
        self._output_buffer = FIFOBundleQueue()

        # Cumulative rows output per input, used for deficit calculation.
        self._rows_seen: List[int] = [0] * len(input_ops)
        self._input_done_flags: List[bool] = [False] * len(input_ops)
        self._stopped: bool = False

        self._stats: StatsDict = {"Mix": []}

        super().__init__(data_context, *input_ops)

    # ------------------------------------------------------------------
    # InternalQueueOperatorMixin interface
    # ------------------------------------------------------------------

    def internal_input_queue_num_blocks(self) -> int:
        return sum(q.num_blocks() for q in self._input_buffers)

    def internal_input_queue_num_bytes(self) -> int:
        return sum(q.estimate_size_bytes() for q in self._input_buffers)

    def internal_output_queue_num_blocks(self) -> int:
        return self._output_buffer.num_blocks()

    def internal_output_queue_num_bytes(self) -> int:
        return self._output_buffer.estimate_size_bytes()

    def clear_internal_input_queue(self) -> None:
        for idx, input_buffer in enumerate(self._input_buffers):
            while input_buffer:
                bundle = input_buffer.get_next()
                self._metrics.on_input_dequeued(bundle, input_index=idx)

    def clear_internal_output_queue(self) -> None:
        while self._output_buffer:
            bundle = self._output_buffer.get_next()
            self._metrics.on_output_dequeued(bundle)

    # ------------------------------------------------------------------
    # PhysicalOperator interface
    # ------------------------------------------------------------------

    def mark_execution_finished(self) -> None:
        # Override InternalQueueOperatorMixin's version to preserve the
        # output buffer for draining. Only clear input queues.
        PhysicalOperator.mark_execution_finished(self)
        self.clear_internal_input_queue()

    def _add_input_inner(self, refs: RefBundle, input_index: int) -> None:
        assert not self.has_completed()
        assert 0 <= input_index < len(self._input_dependencies), input_index
        if self._stopped:
            return
        self._input_buffers[input_index].add(refs)
        self._metrics.on_input_queued(refs, input_index=input_index)
        buf_sizes = [q.num_blocks() for q in self._input_buffers]
        print(
            f"[MIX] add_input(index={input_index}, rows={refs.num_rows()}) "
            f"buf_sizes={buf_sizes} rows_seen={self._rows_seen} "
            f"done_flags={self._input_done_flags}"
        )
        self._try_deficit_output()

    def input_done(self, input_index: int) -> None:
        buf_sizes = [q.num_blocks() for q in self._input_buffers]
        print(
            f"[MIX] input_done(index={input_index}) "
            f"buf_sizes={buf_sizes} rows_seen={self._rows_seen} "
            f"done_flags={self._input_done_flags}"
        )
        self._input_done_flags[input_index] = True
        self._try_deficit_output()

    def all_inputs_done(self) -> None:
        super().all_inputs_done()
        self._try_deficit_output()

    def has_next(self) -> bool:
        return len(self._output_buffer) > 0

    def _get_next_inner(self) -> RefBundle:
        refs = self._output_buffer.get_next()
        self._metrics.on_output_dequeued(refs)
        return refs

    def num_outputs_total(self) -> Optional[int]:
        num_outputs = 0
        for input_op in self.input_dependencies:
            input_num_outputs = input_op.num_outputs_total()
            if input_num_outputs is None:
                return None
            num_outputs += input_num_outputs
        return num_outputs

    def num_output_rows_total(self) -> Optional[int]:
        total_rows = 0
        for input_op in self.input_dependencies:
            input_num_rows = input_op.num_output_rows_total()
            if input_num_rows is None:
                return None
            total_rows += input_num_rows
        return total_rows

    def get_stats(self) -> StatsDict:
        return self._stats

    # ------------------------------------------------------------------
    # Deficit-adjusted output selection
    # ------------------------------------------------------------------

    def _is_input_exhausted(self, index: int) -> bool:
        """An input is exhausted when it's done and its buffer is empty."""
        return (
            self._input_done_flags[index] and not self._input_buffers[index].has_next()
        )

    def _try_deficit_output(self) -> None:
        """Move blocks from input buffers to the output buffer using
        deficit-adjusted selection.

        Computes the deficit for each non-exhausted input and selects the
        one that has fallen furthest behind its target row ratio. If that
        input has blocks available, one block is moved to the output buffer.
        If it has no blocks yet (but isn't exhausted), we return and wait
        rather than outputting from a lower-deficit input — this keeps the
        output sequence deterministic regardless of block arrival timing.
        """
        if self._stopped:
            return

        while True:
            # Check stopping condition before selecting.
            if self._stopping_condition == StoppingCondition.STOP_ON_SHORTEST:
                exhausted = [
                    i
                    for i in range(len(self._input_buffers))
                    if self._is_input_exhausted(i)
                ]
                if exhausted:
                    print(
                        f"[MIX] STOP_ON_SHORTEST triggered: "
                        f"exhausted inputs={exhausted} "
                        f"rows_seen={self._rows_seen}"
                    )
                    self._stopped = True
                    self.mark_execution_finished()
                    return

            # Compute deficits for all non-exhausted inputs.
            total = sum(self._rows_seen)
            best_index = -1
            best_deficit = float("-inf")
            best_weight = -1.0

            for i in range(len(self._input_buffers)):
                if self._is_input_exhausted(i):
                    continue

                deficit = self._weights[i] * total - self._rows_seen[i]
                # Tiebreak by weight (prefer higher weight), then by index.
                if deficit > best_deficit or (
                    deficit == best_deficit and self._weights[i] > best_weight
                ):
                    best_deficit = deficit
                    best_weight = self._weights[i]
                    best_index = i

            if best_index == -1:
                # All inputs are exhausted.
                return

            if not self._input_buffers[best_index].has_next():
                # The highest-deficit input has no blocks yet. Wait for
                # more blocks rather than outputting from a lower-deficit
                # input, to keep the output sequence deterministic
                # and weight ratio accurate.
                buf_sizes = [q.num_blocks() for q in self._input_buffers]
                print(
                    f"[MIX] waiting for input {best_index} (deficit={best_deficit:.1f}) "
                    f"buf_sizes={buf_sizes} done_flags={self._input_done_flags}"
                )
                return

            # Move one block from the selected input to the output buffer.
            bundle = self._input_buffers[best_index].get_next()
            self._metrics.on_input_dequeued(bundle, input_index=best_index)

            num_rows = bundle.num_rows()
            if num_rows is not None:
                self._rows_seen[best_index] += num_rows

            buf_sizes = [q.num_blocks() for q in self._input_buffers]
            print(
                f"[MIX] output from input {best_index} ({num_rows} rows) "
                f"rows_seen={self._rows_seen} buf_sizes={buf_sizes}"
            )

            self._output_buffer.add(bundle)
            self._metrics.on_output_queued(bundle)

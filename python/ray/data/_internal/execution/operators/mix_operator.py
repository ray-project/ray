from typing import List, Optional

from typing_extensions import override

from ray.data._internal.execution.bundle_queue import BaseBundleQueue, FIFOBundleQueue
from ray.data._internal.execution.interfaces import (
    PhysicalOperator,
    RefBundle,
)
from ray.data._internal.execution.operators.base_physical_operator import (
    InternalQueueOperatorMixin,
    NAryOperator,
)
from ray.data._internal.logical.operators.n_ary_operator import (
    MixStoppingCondition,
    estimate_num_mix_outputs,
)
from ray.data._internal.stats import StatsDict
from ray.data.context import DataContext


class MixOperator(InternalQueueOperatorMixin, NAryOperator):
    """An operator that interleaves blocks from multiple input operators
    into a single output stream, respecting target row ratios specified
    by weights.

    Tracks cumulative row counts per input and always pulls from whichever
    input has fallen furthest behind its target ratio. This ensures the
    output row ratio converges to the target weights regardless of input
    block size variance.
    """

    def __init__(
        self,
        data_context: DataContext,
        *input_ops: PhysicalOperator,
        weights: List[float],
        stopping_condition: MixStoppingCondition = MixStoppingCondition.STOP_ON_SHORTEST,
    ):
        assert len(input_ops) >= 1
        assert len(weights) == len(input_ops)
        if any(w <= 0 for w in weights):
            raise ValueError("Weights must be positive.")

        total_weight = sum(weights)
        self._weights = [w / total_weight for w in weights]

        self._stopping_condition = stopping_condition

        self._input_buffers: List[BaseBundleQueue] = [
            FIFOBundleQueue() for _ in range(len(input_ops))
        ]
        self._output_buffer: BaseBundleQueue = FIFOBundleQueue()

        # Cumulative rows output per input, used for deficit calculation.
        self._rows_seen: List[int] = [0] * len(input_ops)
        self._input_done_flags: List[bool] = [False] * len(input_ops)
        self._stopped: bool = False

        self._stats: StatsDict = {"Mix": []}

        super().__init__(data_context, *input_ops)

    # ------------------------------------------------------------------
    # InternalQueueOperatorMixin interface
    # ------------------------------------------------------------------

    @property
    @override
    def _input_queues(self) -> List[BaseBundleQueue]:
        return self._input_buffers

    @property
    @override
    def _output_queues(self) -> List[BaseBundleQueue]:
        return [self._output_buffer]

    # ------------------------------------------------------------------
    # PhysicalOperator interface
    # ------------------------------------------------------------------

    @override
    def mark_execution_finished(self) -> None:
        # Override InternalQueueOperatorMixin's version to preserve the
        # output buffer for draining. Only clear input queues.
        PhysicalOperator.mark_execution_finished(self)
        self.clear_internal_input_queue()

    @override
    def _add_input_inner(self, refs: RefBundle, input_index: int) -> None:
        assert not self.has_completed()
        assert 0 <= input_index < len(self._input_dependencies), input_index
        if self._stopped:
            return
        self._input_buffers[input_index].add(refs)
        self._metrics.on_input_queued(refs, input_index=input_index)
        self._try_output()

    @override
    def input_done(self, input_index: int) -> None:
        self._input_done_flags[input_index] = True
        self._try_output()

    @override
    def all_inputs_done(self) -> None:
        super().all_inputs_done()
        self._try_output()

    @override
    def has_next(self) -> bool:
        return len(self._output_buffer) > 0

    @override
    def _get_next_inner(self) -> RefBundle:
        refs = self._output_buffer.get_next()
        self._metrics.on_output_dequeued(refs)
        return refs

    @override
    def num_outputs_total(self) -> Optional[int]:
        return estimate_num_mix_outputs(
            [op.num_outputs_total() for op in self.input_dependencies],
            self._weights,
            self._stopping_condition,
        )

    @override
    def num_output_rows_total(self) -> Optional[int]:
        return estimate_num_mix_outputs(
            [op.num_output_rows_total() for op in self.input_dependencies],
            self._weights,
            self._stopping_condition,
        )

    @override
    def get_stats(self) -> StatsDict:
        return self._stats

    @override
    def throttling_disabled(self) -> bool:
        # TODO: Disable throttling along with Union once NAry operator resource accounting is fixed.
        return False

    # ------------------------------------------------------------------
    # Output selection
    # ------------------------------------------------------------------

    def _is_input_exhausted(self, index: int) -> bool:
        """An input is exhausted when it's done and its buffer is empty."""
        return (
            self._input_done_flags[index] and not self._input_buffers[index].has_next()
        )

    def _select_most_behind_input(self) -> int:
        """Select which input to pull from next.

        Returns the index of the non-exhausted input that has fallen furthest
        behind its target row ratio. Ties are broken by weight (prefer higher),
        then by index. Returns -1 if all inputs are exhausted.
        """
        total = sum(self._rows_seen)
        best_index = -1
        most_behind = float("-inf")
        best_weight = -1.0

        for i in range(len(self._input_buffers)):
            if self._is_input_exhausted(i):
                continue
            # How far behind this input is: positive means underrepresented.
            gap = self._weights[i] * total - self._rows_seen[i]
            if gap > most_behind or (
                gap == most_behind and self._weights[i] > best_weight
            ):
                most_behind = gap
                best_weight = self._weights[i]
                best_index = i

        return best_index

    def _try_output(self) -> None:
        """Move blocks from input buffers to the output buffer.

        On each iteration, selects the input furthest behind its target ratio.
        If that input has blocks, one is moved to the output. If not, we wait
        rather than pulling from a different input — this keeps the output
        deterministic regardless of block arrival timing.
        """
        if self._stopped:
            return

        while True:
            if self._stopping_condition == MixStoppingCondition.STOP_ON_SHORTEST:
                if any(
                    self._is_input_exhausted(i) for i in range(len(self._input_buffers))
                ):
                    self._stopped = True
                    self.mark_execution_finished()
                    return
            elif self._stopping_condition != MixStoppingCondition.STOP_ON_LONGEST_DROP:
                raise ValueError(
                    f"Unknown stopping condition: {self._stopping_condition}"
                )

            best_index = self._select_most_behind_input()
            if best_index == -1:
                return

            if not self._input_buffers[best_index].has_next():
                # Selected input has no blocks yet — wait rather than
                # pulling from a lower-deficit input.
                return

            # Move one block from the selected input to the output buffer.
            bundle = self._input_buffers[best_index].get_next()
            self._metrics.on_input_dequeued(bundle, input_index=best_index)

            num_rows = bundle.num_rows()
            assert num_rows is not None
            self._rows_seen[best_index] += num_rows

            self._output_buffer.add(bundle)
            self._metrics.on_output_queued(bundle)

from typing import List, Optional

from ray.data._internal.execution.bundle_queue import FIFOBundleQueue
from ray.data._internal.execution.interfaces import (
    ExecutionOptions,
    PhysicalOperator,
    RefBundle,
)
from ray.data._internal.execution.operators.base_physical_operator import (
    InternalQueueOperatorMixin,
    NAryOperator,
)
from ray.data._internal.stats import StatsDict
from ray.data.context import DataContext


class UnionOperator(InternalQueueOperatorMixin, NAryOperator):
    """An operator that combines output blocks from
    two or more input operators into a single output."""

    def __init__(
        self,
        data_context: DataContext,
        *input_ops: PhysicalOperator,
    ):
        """Create a UnionOperator.

        Args:
            input_ops: Operators generating input data for this operator to union.
        """

        # By default, union does not preserve the order of output blocks.
        # To preserve the order, configure ExecutionOptions accordingly.
        self._preserve_order = False

        # Intermediary buffers used to store blocks from each input dependency.
        # Only used when `self._prserve_order` is True.
        self._input_buffers: List["FIFOBundleQueue"] = [
            FIFOBundleQueue() for _ in range(len(input_ops))
        ]

        self._input_done_flags: List[bool] = [False] * len(input_ops)

        self._output_buffer = FIFOBundleQueue()
        self._stats: StatsDict = {"Union": []}
        self._current_input_index = 0
        super().__init__(data_context, *input_ops)

    def start(self, options: ExecutionOptions):
        # Whether to preserve deterministic ordering of output blocks.
        # When True, blocks are emitted in round-robin order across inputs,
        # ensuring the same input always produces the same output order.
        self._preserve_order = options.preserve_order
        super().start(options)

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

    def internal_input_queue_num_blocks(self) -> int:
        return sum(q.num_blocks() for q in self._input_buffers)

    def internal_input_queue_num_bytes(self) -> int:
        return sum(q.estimate_size_bytes() for q in self._input_buffers)

    def internal_output_queue_num_blocks(self) -> int:
        return self._output_buffer.num_blocks()

    def internal_output_queue_num_bytes(self) -> int:
        return self._output_buffer.estimate_size_bytes()

    def clear_internal_input_queue(self) -> None:
        """Clear internal input queues."""
        for input_buffer in self._input_buffers:
            while input_buffer:
                bundle = input_buffer.get_next()
                self._metrics.on_input_dequeued(bundle)

    def clear_internal_output_queue(self) -> None:
        """Clear internal output queue."""
        while self._output_buffer:
            bundle = self._output_buffer.get_next()
            self._metrics.on_output_dequeued(bundle)

    def _add_input_inner(self, refs: RefBundle, input_index: int) -> None:
        assert not self.has_completed()
        assert 0 <= input_index <= len(self._input_dependencies), input_index
        if self._preserve_order:
            self._input_buffers[input_index].add(refs)
            self._metrics.on_input_queued(refs)
            self._try_round_robin()
        else:
            self._output_buffer.add(refs)
            self._metrics.on_output_queued(refs)

    def input_done(self, input_index: int) -> None:
        self._input_done_flags[input_index] = True
        if self._preserve_order:
            self._try_round_robin()

    def all_inputs_done(self) -> None:
        super().all_inputs_done()

        if not self._preserve_order:
            return
        self._try_round_robin()
        assert all(not buffer.has_next() for buffer in self._input_buffers)

    def has_next(self) -> bool:
        # Check if the output buffer still contains at least one block.
        return len(self._output_buffer) > 0

    def _get_next_inner(self) -> RefBundle:
        refs = self._output_buffer.get_next()
        self._metrics.on_output_dequeued(refs)
        return refs

    def get_stats(self) -> StatsDict:
        return self._stats

    def _try_round_robin(self) -> None:
        """Try to move blocks from input buffers to output in round-robin order.

        Pulls one block from the current input, then advances to the next.
        If the current input's buffer is empty but not done, we return
        without advancing to the next input so the scheduling won't be blocked.

        This ensures deterministic ordering of output blocks:
        - We iterate through inputs in a fixed order (0, 1, 2, ..., 0, 1, ...).
        - We only advance to the next input after consuming exactly one block
          from the current input (or if the current input is exhausted).
        - If an input is not ready (empty but not done), we return
          rather than skipping it, preserving the round-robin sequence.
        """
        num_inputs = len(self._input_buffers)

        while True:
            buffer = self._input_buffers[self._current_input_index]

            if buffer.has_next():
                refs = buffer.get_next()
                self._metrics.on_input_dequeued(refs)
                self._output_buffer.add(refs)
                self._metrics.on_output_queued(refs)
            elif not self._input_done_flags[self._current_input_index] or all(
                not buffer.has_next() for buffer in self._input_buffers
            ):
                return

            self._current_input_index = (self._current_input_index + 1) % num_inputs

import collections
from typing import List, Optional

from ray.data._internal.execution.interfaces import (
    ExecutionOptions,
    PhysicalOperator,
    RefBundle,
)
from ray.data._internal.execution.operators.base_physical_operator import NAryOperator
from ray.data._internal.stats import StatsDict
from ray.data.context import DataContext


class UnionOperator(NAryOperator):
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
        self._input_buffers: List[collections.deque[RefBundle]] = [
            collections.deque() for _ in range(len(input_ops))
        ]

        # The index of the input dependency that is currently the source of
        # the output buffer. New inputs from this input dependency will be added
        # directly to the output buffer. Only used when `self._preserve_order` is True.
        self._input_idx_to_output = 0

        self._output_buffer: collections.deque[RefBundle] = collections.deque()
        self._stats: StatsDict = {"Union": []}
        super().__init__(data_context, *input_ops)

    def start(self, options: ExecutionOptions):
        # Whether to preserve the order of the input data (both the
        # order of the input operators and the order of the blocks within).
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

    def _add_input_inner(self, refs: RefBundle, input_index: int) -> None:
        assert not self.completed()
        assert 0 <= input_index <= len(self._input_dependencies), input_index

        if not self._preserve_order:
            self._output_buffer.append(refs)
            self._metrics.on_output_queued(refs)
        else:
            self._input_buffers[input_index].append(refs)
            self._metrics.on_input_queued(refs)

    def all_inputs_done(self) -> None:
        super().all_inputs_done()

        if not self._preserve_order:
            return

        assert len(self._output_buffer) == 0, len(self._output_buffer)
        for input_buffer in self._input_buffers:
            while input_buffer:
                refs = input_buffer.popleft()
                self._metrics.on_input_dequeued(refs)
                self._output_buffer.append(refs)
                self._metrics.on_output_queued(refs)

    def has_next(self) -> bool:
        # Check if the output buffer still contains at least one block.
        return len(self._output_buffer) > 0

    def _get_next_inner(self) -> RefBundle:
        refs = self._output_buffer.popleft()
        self._metrics.on_output_dequeued(refs)
        return refs

    def get_stats(self) -> StatsDict:
        return self._stats

    def implements_accurate_memory_accounting(self):
        return True

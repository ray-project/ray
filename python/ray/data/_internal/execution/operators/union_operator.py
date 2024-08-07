from typing import List, Optional

from ray.data._internal.execution.interfaces import (
    ExecutionOptions,
    PhysicalOperator,
    RefBundle,
)
from ray.data._internal.execution.operators.base_physical_operator import NAryOperator
from ray.data._internal.stats import StatsDict


class UnionOperator(NAryOperator):
    """An operator that combines output blocks from
    two or more input operators into a single output."""

    def __init__(
        self,
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
        self._input_buffers: List[List[RefBundle]] = [[] for _ in range(len(input_ops))]

        # The index of the input dependency that is currently the source of
        # the output buffer. New inputs from this input dependency will be added
        # directly to the output buffer. Only used when `self._preserve_order` is True.
        self._input_idx_to_output = 0

        self._output_buffer: List[RefBundle] = []
        self._stats: StatsDict = {"Union": []}
        super().__init__(*input_ops)

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

    def _add_input_inner(self, refs: RefBundle, input_index: int) -> None:
        assert not self.completed()
        assert 0 <= input_index <= len(self._input_dependencies), input_index

        if not self._preserve_order:
            self._output_buffer.append(refs)
        else:
            if input_index == self._input_idx_to_output:
                self._output_buffer.append(refs)
            else:
                self._input_buffers[input_index].append(refs)

    def input_done(self, input_index: int) -> None:
        """When `self._preserve_order` is True, change the
        output buffer source to the next input dependency
        once the current input dependency calls `input_done()`."""
        if not self._preserve_order:
            return
        if not input_index == self._input_idx_to_output:
            return
        next_input_idx = self._input_idx_to_output + 1
        if next_input_idx < len(self._input_buffers):
            self._output_buffer.extend(self._input_buffers[next_input_idx])
            self._input_buffers[next_input_idx].clear()
            self._input_idx_to_output = next_input_idx
        super().input_done(input_index)

    def all_inputs_done(self) -> None:
        # Note that in the case where order is not preserved, all inputs
        # are directly added to the output buffer as soon as they are received,
        # so there is no need to check any intermediary buffers.
        if self._preserve_order:
            for idx, input_buffer in enumerate(self._input_buffers):
                assert len(input_buffer) == 0, (
                    f"Input at index {idx} still has "
                    f"{len(input_buffer)} blocks remaining."
                )
        super().all_inputs_done()

    def has_next(self) -> bool:
        # Check if the output buffer still contains at least one block.
        return len(self._output_buffer) > 0

    def _get_next_inner(self) -> RefBundle:
        return self._output_buffer.pop(0)

    def get_stats(self) -> StatsDict:
        return self._stats

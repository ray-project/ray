from typing import List, Optional

from ray.data._internal.execution.interfaces import PhysicalOperator, RefBundle
from ray.data._internal.execution.operators.base_physical_operator import NAryOperator
from ray.data._internal.stats import StatsDict


class UnionOperator(NAryOperator):
    """An operator that combines blocks from input operators."""

    def __init__(
        self,
        *input_ops: PhysicalOperator,
    ):
        """Create a UnionOperator.

        Args:
            input_ops: Operators generating input data for this operator to union.
        """
        self._input_buffers: List[List[RefBundle]] = [[] for _ in range(len(input_ops))]
        # Whether the input dependency still has blocks remaining.
        self._input_dependency_has_next: List[bool] = [
            True for _ in range(len(input_ops))
        ]
        # The index of the input dependency that is currently the source of
        # the output buffer. New inputs from this input dependency will be added
        # directly to the output buffer.
        self._input_idx_to_output = None
        self._output_buffer: List[RefBundle] = []
        self._stats: StatsDict = {}
        super().__init__(*input_ops)

    def num_outputs_total(self) -> Optional[int]:
        num_outputs = 0
        for input_op in self.input_dependencies:
            op_num_outputs = input_op.num_outputs_total()
            # If at least one of the input ops has an unknown number of outputs,
            # the number of outputs of the union operator is unknown.
            if op_num_outputs is None:
                return None
            num_outputs += op_num_outputs
        return num_outputs

    def add_input(self, refs: RefBundle, input_index: int) -> None:
        assert not self.completed()
        assert 0 <= input_index <= len(self._input_dependencies), input_index
        if self._input_idx_to_output is None:
            self._input_idx_to_output = input_index

        # If the input block is from the current source of the output buffer,
        # add it directly to the output buffer. Otherwise, save them in the
        # corresponding input buffer.
        if self._input_idx_to_output == input_index:
            self._output_buffer.append(refs)
        else:
            self._input_buffers[input_index].append(refs)

        # If the current input dependency no longer has any remaining outputs,
        # mark it as such, and add its remaining blocks to the output buffer.
        if not self._input_dependencies[input_index].has_next():
            self._input_dependency_has_next[input_index] = False
            self._output_buffer.extend(self._input_buffers[input_index])
            # Clear the input buffer, since all its blocks
            # have now been added to the output buffer.
            self._input_buffers[input_index].clear()
            self._change_output_buffer_source()

    def inputs_done(self) -> None:
        # Move the remaining blocks from the input buffers to the output buffer.
        for idx, input_buffer in enumerate(self._input_buffers):
            if self._input_dependency_has_next[idx]:
                self._output_buffer.extend(input_buffer)
                self._input_dependency_has_next[idx] = False
                input_buffer.clear()
        self._input_buffers.clear()
        super().inputs_done()

    def has_next(self) -> bool:
        # If the output buffer still contains at least one block, return True.
        if len(self._output_buffer) > 0:
            return True

        # If the output buffer is empty, check if at least one of the input
        # buffers still has blocks remaining.
        if any(self._input_dependency_has_next):
            self._change_output_buffer_source()
        return len(self._output_buffer) > 0

    def get_next(self) -> RefBundle:
        return self._output_buffer.pop(0)

    def get_stats(self) -> StatsDict:
        return self._stats

    def _change_output_buffer_source(self):
        """Change the source of the output buffer to the input buffer
        with the most blocks remaining, adding its blocks to the output buffer
        and clearing the input buffer."""

        # Use the input buffer with the most blocks as
        # the next source for the output buffer.
        num_blocks_in_input_buffers = [len(b) for b in self._input_buffers]
        self._input_idx_to_output = max(
            range(len(num_blocks_in_input_buffers)),
            key=num_blocks_in_input_buffers.__getitem__,
        )
        # Add the existing blocks in the new input buffer source to the output buffer.
        self._output_buffer.extend(self._input_buffers[self._input_idx_to_output])

        # Clear the input buffer, since all its blocks have now been
        # added to the output buffer.
        self._input_buffers[self._input_idx_to_output].clear()

from typing import Callable, List, Optional, Tuple

from ray.data._internal.execution.interfaces import PhysicalOperator, RefBundle
from ray.data._internal.execution.operators.base_physical_operator import NAryOperator
from ray.data._internal.stats import StatsDict
from ray.data.block import BlockMetadata


class UnionOperator(NAryOperator):
    """An operator that combines blocks from input operators.

    The order of the blocks from the input operators is preserved,
    as is the relative ordering between the input operators.
    """

    def __init__(
        self,
        *input_ops: PhysicalOperator,
    ):
        """Create a UnionOperator.

        Args:
            input_ops: Operators generating input data for this operator to union.
        """

        self._input_buffers: List[List[RefBundle]] = [[] for _ in range(len(input_ops))]
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
        self._input_buffers[input_index].append(refs)

    def inputs_done(self) -> None:
        self._output_buffer, self._stats = self._union(self._input_buffers)
        self._input_buffers.clear()
        super().inputs_done()

    def has_next(self) -> bool:
        return len(self._output_buffer) > 0

    def get_next(self) -> RefBundle:
        return self._output_buffer.pop(0)

    def get_stats(self) -> StatsDict:
        return self._stats

    def get_transformation_fn(self) -> Callable:
        return self._union

    def _union(
        self, inputs: List[List[RefBundle]]
    ) -> Tuple[List[RefBundle], StatsDict]:
        output_refs: List[RefBundle] = []
        output_metadata: List[BlockMetadata] = []
        # Collect all the refs and metadata from the input refs.
        for curr_input_refs in inputs:
            output_refs.extend(curr_input_refs)
            for ref_bundle in curr_input_refs:
                for _, block_metadata in ref_bundle.blocks:
                    output_metadata.append(block_metadata)

        stats = {self._name: output_metadata}
        return output_refs, stats

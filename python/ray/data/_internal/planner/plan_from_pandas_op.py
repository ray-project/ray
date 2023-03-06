from typing import List

import ray
from ray.data._internal.execution.interfaces import (
    PhysicalOperator,
    RefBundle,
)
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.logical.operators.from_pandas_operator import FromPandasRefs
from ray.data.block import BlockAccessor, BlockExecStats
from ray.data.context import DatasetContext


def _plan_from_pandas_refs_op(op: FromPandasRefs) -> PhysicalOperator:
    """Get the corresponding DAG of physical operators for FromPandasRefs.

    Note this method only converts the given `op`, but not its input dependencies.
    See Planner.plan() for more details.
    """

    def get_input_data() -> List[RefBundle]:
        ref_bundles: List[RefBundle] = []

        context = DatasetContext.get_current()
        for df in op._dfs:
            if context.enable_pandas_block:
                block = df
            else:
                import pyarrow as pa

                block = pa.table(df)
            stats = BlockExecStats.builder()
            block_metadata = BlockAccessor.for_block(block).get_metadata(
                input_files=None, exec_stats=stats.build()
            )
            ref_bundles.append(
                RefBundle([ray.put(block), block_metadata], owns_blocks=True)
            )
        return ref_bundles

    return InputDataBuffer(input_data_factory=get_input_data)

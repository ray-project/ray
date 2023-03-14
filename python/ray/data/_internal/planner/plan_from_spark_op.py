from typing import List

import ray
from ray.data._internal.execution.interfaces import (
    PhysicalOperator,
    RefBundle,
)
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.logical.operators.from_spark_operator import FromSpark
from ray.data.block import BlockAccessor, BlockExecStats


def _plan_from_spark_op(op: FromSpark) -> PhysicalOperator:
    """Get the corresponding DAG of physical operators for `FromSpark`.

    Note this method only converts the given `op`, but not its input dependencies.
    See Planner.plan() for more details.
    """

    def get_input_data() -> List[RefBundle]:
        from raydp.spark.dataset import _save_spark_df_to_object_store

        num_part = op._df.rdd.getNumPartitions()
        if op._parallelism is not None:
            if op._parallelism != num_part:
                df = op._df.repartition(op._parallelism)
        blocks, _ = _save_spark_df_to_object_store(df, False)

        ref_bundles: List[RefBundle] = []
        for block in blocks:
            stats = BlockExecStats.builder()
            block_metadata = BlockAccessor.for_block(block).get_metadata(
                input_files=None, exec_stats=stats.build()
            )
            ref_bundles.append(
                RefBundle([ray.put(block), block_metadata], owns_blocks=True)
            )
        return ref_bundles

    return InputDataBuffer(input_data_factory=get_input_data)

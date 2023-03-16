from typing import List, Union

import ray
from ray.data._internal.execution.interfaces import (
    PhysicalOperator,
    RefBundle,
)
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.logical.operators.from_arrow_operator import (
    FromArrowRefs,
    FromSpark,
)
from ray.data.block import BlockAccessor, BlockExecStats
from ray.types import ObjectRef


def _plan_from_arrow_refs_op(op: Union[FromArrowRefs, FromSpark]) -> PhysicalOperator:
    """Get the corresponding DAG of physical operators for `FromArrowRefs`.

    Note this method only converts the given `op`, but not its input dependencies.
    See Planner.plan() for more details.
    """

    def get_input_data() -> List[RefBundle]:
        if isinstance(op, FromSpark):
            from raydp.spark.dataset import _save_spark_df_to_object_store

            num_part = op._df.rdd.getNumPartitions()
            if op._parallelism is not None:
                if op._parallelism != num_part:
                    df = op._df.repartition(op._parallelism)
            blocks, _ = _save_spark_df_to_object_store(df, False)
            op._tables = blocks

        ref_bundles: List[RefBundle] = []
        for idx, table_ref in enumerate(op._tables):
            if not isinstance(table_ref, ObjectRef):
                op._tables[idx] = ray.put(table_ref)
                table_ref = op._tables[idx]
            stats = BlockExecStats.builder()
            block_metadata = BlockAccessor.for_block(table_ref).get_metadata(
                input_files=None, exec_stats=stats.build()
            )
            ref_bundles.append(RefBundle([table_ref, block_metadata], owns_blocks=True))
        return ref_bundles

    return InputDataBuffer(input_data_factory=get_input_data)

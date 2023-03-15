from typing import List, Union

import ray
from ray.data._internal.execution.interfaces import (
    PhysicalOperator,
    RefBundle,
)
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.logical.operators.from_pandas_operator import (
    FromPandasRefs,
    FromDask,
)
from ray.data.block import BlockAccessor, BlockExecStats
from ray.data.context import DatasetContext
from ray.types import ObjectRef


def _plan_from_pandas_refs_op(op: Union[FromPandasRefs, FromDask]) -> PhysicalOperator:
    """Get the corresponding DAG of physical operators for FromPandasRefs.

    Note this method only converts the given `op`, but not its input dependencies.
    See Planner.plan() for more details.
    """

    def get_input_data() -> List[RefBundle]:
        if isinstance(op, FromDask):
            import dask
            from ray.util.dask import ray_dask_get
            import pandas

            partitions = op._df.to_delayed()
            persisted_partitions = dask.persist(*partitions, scheduler=ray_dask_get)

            def to_ref(df):
                if isinstance(df, pandas.DataFrame):
                    return ray.put(df)
                elif isinstance(df, ray.ObjectRef):
                    return df
                else:
                    raise ValueError(
                        "Expected a Ray object ref or a Pandas DataFrame, "
                        f"got {type(df)}"
                    )

            op._dfs = [
                to_ref(next(iter(part.dask.values()))) for part in persisted_partitions
            ]
        ref_bundles: List[RefBundle] = []

        context = DatasetContext.get_current()
        for df_ref in op._dfs:
            if not isinstance(df_ref, ObjectRef):
                df_ref = ray.put(df_ref)

            if context.enable_pandas_block:
                block = df_ref
            else:
                import pyarrow as pa

                block = pa.table(df_ref)

            stats = BlockExecStats.builder()
            block_metadata = BlockAccessor.for_block(block).get_metadata(
                input_files=None, exec_stats=stats.build()
            )
            ref_bundles.append(
                RefBundle([ray.put(block), block_metadata], owns_blocks=True)
            )
        return ref_bundles

    return InputDataBuffer(input_data_factory=get_input_data)

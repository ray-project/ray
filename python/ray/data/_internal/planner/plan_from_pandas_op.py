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
    FromModin,
)
from ray.data.context import DatasetContext
from ray import ObjectRef
from ray.data._internal.remote_fn import cached_remote_fn
from ray.data._internal.util import (
    _df_to_block,
    _get_metadata,
)

get_metadata = cached_remote_fn(_get_metadata)
df_to_block = cached_remote_fn(_df_to_block, num_returns=2)


def _plan_from_pandas_refs_op(
    op: Union[FromPandasRefs, FromDask, FromModin]
) -> PhysicalOperator:
    """Get the corresponding DAG of physical operators for FromPandasRefs.

    Note this method only converts the given `op`, but not its input dependencies.
    See Planner.plan() for more details.
    """

    def _init_data_from_dask(op: FromDask):
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

    def _init_data_from_modin(op: FromModin):
        from modin.distributed.dataframe.pandas.partitions import unwrap_partitions

        op._dfs = unwrap_partitions(op._df, axis=0)

    def get_input_data() -> List[RefBundle]:
        if isinstance(op, FromDask):
            _init_data_from_dask(op)
        elif isinstance(op, FromModin):
            _init_data_from_modin(op)

        context = DatasetContext.get_current()
        ref_bundles: List[RefBundle] = []
        for idx, df_ref in enumerate(op._dfs):
            if not isinstance(df_ref, ObjectRef):
                op._dfs[idx] = ray.put(df_ref)
                df_ref = op._dfs[idx]

            if context.enable_pandas_block:
                block = df_ref
                block_metadata = get_metadata.remote(df_ref)
            else:
                block, block_metadata = df_to_block.remote(df_ref)
            ref_bundles.append(
                RefBundle([(block, ray.get(block_metadata))], owns_blocks=True)
            )
        return ref_bundles

    return InputDataBuffer(input_data_factory=get_input_data)

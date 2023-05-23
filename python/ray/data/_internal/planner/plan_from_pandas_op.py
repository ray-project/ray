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
    FromMars,
)
from ray.data.context import DataContext

FromPandasRefsOperators = Union[FromPandasRefs, FromDask, FromModin, FromMars]


def _plan_from_pandas_refs_op(op: FromPandasRefsOperators) -> PhysicalOperator:
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

    def _init_data_from_mars(op: FromMars):
        from mars.dataframe.contrib.raydataset import get_chunk_refs

        op._dfs = get_chunk_refs(op._df)

    def get_input_data() -> List[RefBundle]:
        from ray.data._internal.remote_fn import cached_remote_fn
        from ray.data._internal.util import (
            pandas_df_to_arrow_block,
            get_table_block_metadata,
        )

        owns_blocks = True
        if isinstance(op, FromDask):
            _init_data_from_dask(op)
        elif isinstance(op, FromModin):
            _init_data_from_modin(op)
        elif isinstance(op, FromMars):
            _init_data_from_mars(op)
            # MARS holds the MARS dataframe in memory in `to_ray_dataset()`
            # to avoid object GC, so this operator cannot not own the blocks.
            owns_blocks = False

        context = DataContext.get_current()

        if context.enable_pandas_block:
            get_metadata = cached_remote_fn(get_table_block_metadata)
            metadata = ray.get([get_metadata.remote(df_ref) for df_ref in op._dfs])
            return [
                RefBundle([(block, block_metadata)], owns_blocks=owns_blocks)
                for block, block_metadata in zip(op._dfs, metadata)
            ]

        df_to_block = cached_remote_fn(pandas_df_to_arrow_block, num_returns=2)
        res = [df_to_block.remote(df) for df in op._dfs]
        blocks, metadata = map(list, zip(*res))
        metadata = ray.get(metadata)
        return [
            RefBundle([(block, block_metadata)], owns_blocks=owns_blocks)
            for block, block_metadata in zip(blocks, metadata)
        ]

    return InputDataBuffer(input_data_factory=get_input_data)

from typing import TYPE_CHECKING, List

from ray.data._internal.logical.interfaces import LogicalOperator
from ray.types import ObjectRef
import ray

if TYPE_CHECKING:
    import pandas
    import dask
    import modin
    import mars


class FromPandasRefs(LogicalOperator):
    """Logical operator for `from_pandas_refs`."""

    def __init__(
        self, dfs: List[ObjectRef["pandas.DataFrame"]], op_name: str = "FromPandasRefs"
    ):
        super().__init__(op_name, [])
        self._dfs = dfs


class FromPandas(FromPandasRefs):
    """Logical operator for `from_pandas`."""

    def __init__(
        self,
        dfs: List["pandas.DataFrame"],
    ):
        super().__init__([ray.put(df) for df in dfs], "FromPandas")


class FromMARS(FromPandasRefs):
    """Logical operator for `from_mars`."""

    def __init__(
        self,
        df: "mars.DataFrame",
    ):
        from mars.dataframe.contrib.raydataset import get_chunk_refs

        super().__init__(get_chunk_refs(df), "FromMARS")


class FromDask(FromPandasRefs):
    """Logical operator for `from_dask`."""

    def __init__(
        self,
        df: "dask.DataFrame",
    ):
        import dask
        from ray.util.dask import ray_dask_get
        import pandas

        partitions = df.to_delayed()
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

        refs = [to_ref(next(iter(part.dask.values()))) for part in persisted_partitions]
        super().__init__(refs, "FromDask")


class FromModin(FromPandasRefs):
    """Logical operator for `from_modin`."""

    def __init__(
        self,
        df: "modin.DataFrame",
    ):
        from modin.distributed.dataframe.pandas.partitions import unwrap_partitions

        parts = unwrap_partitions(df, axis=0)
        super().__init__(parts, "FromModin")

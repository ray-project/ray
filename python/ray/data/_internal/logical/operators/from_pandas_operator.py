from typing import TYPE_CHECKING, List, Union

from ray.data._internal.logical.interfaces import LogicalOperator
from ray.types import ObjectRef

if TYPE_CHECKING:
    import pandas
    import dask
    import modin
    import mars


class FromPandasRefs(LogicalOperator):
    """Logical operator for `from_pandas_refs`."""

    def __init__(
        self,
        dfs: Union[List[ObjectRef["pandas.DataFrame"]], List["pandas.DataFrame"]],
        op_name: str = "FromPandasRefs",
    ):
        super().__init__(op_name, [])
        self._dfs = dfs


class FromMARS(FromPandasRefs):
    """Logical operator for `from_mars`."""

    def __init__(
        self,
        df: "mars.DataFrame",
    ):
        from mars.dataframe.contrib.raydataset import get_chunk_refs

        super().__init__(get_chunk_refs(df), "FromMARS")


class FromDask(LogicalOperator):
    """Logical operator for `from_dask`."""

    def __init__(
        self,
        df: "dask.DataFrame",
    ):
        self._df = df
        super().__init__("FromDask", [])


class FromModin(FromPandasRefs):
    """Logical operator for `from_modin`."""

    def __init__(
        self,
        df: "modin.DataFrame",
    ):
        from modin.distributed.dataframe.pandas.partitions import unwrap_partitions

        parts = unwrap_partitions(df, axis=0)
        super().__init__(parts, "FromModin")

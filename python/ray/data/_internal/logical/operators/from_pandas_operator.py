from typing import TYPE_CHECKING, List, Union

import ray
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
        self._dfs: List[ObjectRef["pandas.DataFrame"]] = []
        for df_ref in dfs:
            if isinstance(df_ref, ray.ObjectRef):
                self._dfs.append(df_ref)
            else:
                self._dfs.append(ray.put(df_ref))


class FromDask(LogicalOperator):
    """Logical operator for `from_dask`."""

    def __init__(
        self,
        df: "dask.DataFrame",
    ):
        self._df = df
        super().__init__("FromDask", [])


class FromModin(LogicalOperator):
    """Logical operator for `from_modin`."""

    def __init__(
        self,
        df: "modin.DataFrame",
    ):
        self._df = df
        super().__init__("FromModin", [])


class FromMars(LogicalOperator):
    """Logical operator for `from_mars`."""

    def __init__(
        self,
        df: "mars.DataFrame",
    ):
        self._df = df
        super().__init__("FromMars", [])

from typing import TYPE_CHECKING, List

from ray.data._internal.logical.interfaces import LogicalOperator
from ray.types import ObjectRef
import ray

if TYPE_CHECKING:
    import pandas


class FromPandasRefs(LogicalOperator):
    """Logical operator for `from_pandas_ref` (and `from_pandas` by extension)."""

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
        self._dfs = dfs

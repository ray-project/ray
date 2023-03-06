from typing import TYPE_CHECKING, List

from ray.data._internal.logical.interfaces import LogicalOperator
from ray.types import ObjectRef

if TYPE_CHECKING:
    import pandas


class FromPandasRefs(LogicalOperator):
    """Logical operator for from_pandas_ref."""

    def __init__(
        self,
        dfs: List[ObjectRef["pandas.DataFrame"]],
    ):
        super().__init__("FromPandasRefs", [])
        self._dfs = dfs


class FromPandas(LogicalOperator):
    """Logical operator for from_pandas."""

    def __init__(
        self,
        dfs: List["pandas.DataFrame"],
    ):
        super().__init__("FromPandas", [])
        self._dfs = dfs

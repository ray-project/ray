from typing import Optional, TYPE_CHECKING
from ray.data._internal.logical.interfaces import LogicalOperator


if TYPE_CHECKING:
    import pyspark


class FromSpark(LogicalOperator):
    """Logical operator for `from_spark`."""

    def __init__(
        self,
        df: "pyspark.sql.DataFrame",
        parallelism: Optional[int] = None,
    ):
        self._parallelism = parallelism
        self._df = df
        super().__init__("FromSpark", [])

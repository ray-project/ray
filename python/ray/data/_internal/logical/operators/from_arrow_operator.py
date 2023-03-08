from typing import TYPE_CHECKING, List, Optional, Union

import ray
from ray.data._internal.logical.interfaces import LogicalOperator
from ray.types import ObjectRef

if TYPE_CHECKING:
    import pyarrow
    import pyspark


class FromArrowRefs(LogicalOperator):
    """Logical operator for `from_arrow_refs`."""

    def __init__(
        self,
        tables: List[ObjectRef[Union["pyarrow.Table", bytes]]],
        op_name: str = "FromArrowRefs",
    ):
        super().__init__(op_name, [])
        self._tables = tables


class FromArrow(FromArrowRefs):
    """Logical operator for `from_arrow`."""

    def __init__(
        self,
        tables: List[Union["pyarrow.Table", bytes]],
    ):
        super().__init__([ray.put(t) for t in tables], "FromArrow")


class FromSpark(FromArrowRefs):
    """Logical operator for `from_spark`."""

    def __init__(
        self,
        df: "pyspark.sql.DataFrame",
        parallelism: Optional[int] = None,
    ):
        from raydp.spark.dataset import _save_spark_df_to_object_store

        self._parallelism = parallelism

        num_part = df.rdd.getNumPartitions()
        if parallelism is not None:
            if parallelism != num_part:
                df = df.repartition(parallelism)
        blocks, _ = _save_spark_df_to_object_store(df, False)
        super().__init__(blocks, "FromSpark")

from typing import TYPE_CHECKING, List, Optional, Union

import ray
from ray.data._internal.logical.interfaces import LogicalOperator
from ray.types import ObjectRef

if TYPE_CHECKING:
    import pyarrow
    import pyspark
    import datasets


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
        self, tables: List[Union["pyarrow.Table", bytes]], op_name: str = "FromArrow"
    ):
        super().__init__([ray.put(t) for t in tables], op_name)


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


class FromHuggingFace(FromArrow):
    """Logical operator for `from_huggingface`."""

    def __init__(
        self,
        dataset: Union["datasets.Dataset", "datasets.DatasetDict"],
    ):
        from datasets import Dataset, DatasetDict

        if isinstance(dataset, DatasetDict):
            dataset_dict_keys = list(dataset.keys())
            # For each Dataset in the DatasetDict, track the order of keys
            self.dataset_dict_keys = dataset_dict_keys
            dataset_dict_ds = list(dataset.values())
            super().__init__(dataset_dict_ds, "FromHuggingFace")
        elif isinstance(dataset, Dataset):
            super().__init__([dataset.data.table], "FromHuggingFace")
            # For single Datasets, we don't have any keys to track
            self.dataset_dict_keys = None
        else:
            raise TypeError(
                "`dataset` must be a `datasets.Dataset` or `datasets.DatasetDict`, "
                f"got {type(dataset)}"
            )

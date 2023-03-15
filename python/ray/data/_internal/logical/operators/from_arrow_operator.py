from typing import TYPE_CHECKING, List, Optional, Union

from ray.data._internal.logical.interfaces import LogicalOperator
from ray.types import ObjectRef

if TYPE_CHECKING:
    import pyarrow
    import datasets
    import pyspark

ArrowTable = Union["pyarrow.Table", bytes]
ArrowTableOrRefsList = Union[List[ObjectRef[ArrowTable]], List[ArrowTable]]


class FromArrowRefs(LogicalOperator):
    """Logical operator for `from_arrow_refs`."""

    def __init__(
        self,
        tables: ArrowTableOrRefsList,
        op_name: str = "FromArrowRefs",
    ):
        super().__init__(op_name, [])
        self._tables = tables


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


class FromHuggingFace(FromArrowRefs):
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

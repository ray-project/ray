from typing import TYPE_CHECKING, List, Union

import ray
from ray.data._internal.logical.interfaces import LogicalOperator
from ray.types import ObjectRef

if TYPE_CHECKING:
    import pyarrow
    import datasets

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
        self._tables: List[ObjectRef[ArrowTable]] = []
        for table_or_ref in tables:
            if isinstance(table_or_ref, ray.ObjectRef):
                self._tables.append(table_or_ref)
            else:
                self._tables.append(ray.put(table_or_ref))


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

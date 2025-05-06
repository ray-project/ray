import abc
from typing import Dict, Generic, Optional, TypeVar, Union, List, TYPE_CHECKING

import numpy as np

from ray.data.block import DataBatch
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    import torch
    import pyarrow
    import pandas

    from ray.data.dataset import CollatedData


DataBatchType = TypeVar("DataBatchType", bound=DataBatch)


@DeveloperAPI
class CollateFn(Generic[DataBatchType]):
    """Abstract interface for collate_fn for `iter_torch_batches`. See doc-string of
    `collate_fn` in `iter_torch_batches` API for more details.
    """

    @abc.abstractmethod
    def __call__(self, batch: DataBatchType) -> "CollatedData":
        """Convert a batch of data to collated format.

        Args:
            batch: The input batch to collate.

        Returns:
            The collated data in the format expected by the model.
        """
        ...


@DeveloperAPI
class ArrowBatchCollateFn(CollateFn["pyarrow.Table"]):
    """Collate function that takes pyarrow.Table as the input batch type."""

    def __call__(self, batch: "pyarrow.Table") -> "CollatedData":
        """Convert a batch of pyarrow.Table to collated format.

        Args:
            batch: The input pyarrow.Table batch to collate.

        Returns:
            The collated data in the format expected by the model.
        """
        ...


@DeveloperAPI
class NumpyBatchCollateFn(CollateFn[Dict[str, np.ndarray]]):
    """Collate function that takes a dictionary of numpy arrays as the input batch type."""

    def __call__(self, batch: Dict[str, np.ndarray]) -> "CollatedData":
        """Convert a batch of numpy arrays to collated format.

        Args:
            batch: The input dictionary of numpy arrays batch to collate.

        Returns:
            The collated data in the format expected by the model.
        """
        ...


@DeveloperAPI
class PandasBatchCollateFn(CollateFn["pandas.DataFrame"]):
    """Collate function that takes a pandas.DataFrame as the input batch type."""

    def __call__(self, batch: "pandas.DataFrame") -> "CollatedData":
        """Convert a batch of pandas.DataFrame to collated format.

        Args:
            batch: The input pandas.DataFrame batch to collate.

        Returns:
            The collated data in the format expected by the model.
        """
        ...


@DeveloperAPI
class DefaultCollateFn(ArrowBatchCollateFn):
    """Default collate function for converting Arrow batches to PyTorch tensors."""

    def __init__(
        self,
        dtypes: Optional[Union["torch.dtype", Dict[str, "torch.dtype"]]] = None,
        device: Optional[Union[str, "torch.device"]] = None,
    ):
        """Initialize the collate function.

        Args:
            dtypes: The torch dtype(s) for the created tensor(s); if None, the dtype
                will be inferred from the tensor data.
            device: The device on which the tensor should be placed. Can be a string
                (e.g. "cpu", "cuda:0") or a torch.device object.
        """
        super().__init__()
        self.dtypes = dtypes
        if isinstance(device, str):
            self.device = torch.device(device)
        else:
            self.device = device

    def __call__(self, batch: "pyarrow.Table") -> Dict[str, List["torch.Tensor"]]:
        """Convert an Arrow batch to PyTorch tensors.

        Args:
            batch: PyArrow Table to convert

        Returns:
            Dictionary mapping column names to lists of tensors
        """
        from ray.air._internal.torch_utils import (
            arrow_batch_to_tensors,
        )

        # For GPU transfer, we can skip the combining chunked arrays. This is because
        # we can convert the chunked arrays to corresponding numpy format and then to
        # Tensors and transfer the corresponding list of Tensors to GPU directly.
        # However, for CPU transfer, we need to combine the chunked arrays first
        # before converting to numpy format and then to Tensors.
        combine_chunks = self.device.type == "cpu"
        return arrow_batch_to_tensors(
            batch, dtypes=self.dtypes, combine_chunks=combine_chunks
        )

import abc
from typing import Dict, Generic, Optional, TypeVar, Union, List, TYPE_CHECKING, Any

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
    """Abstract interface for collate_fn for iter_torch_batches. See doc-string of
    `collate_fn` for more details.
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
class DefaultArrowCollateFn(ArrowBatchCollateFn):
    """Default collate function for converting Arrow batches to PyTorch tensors."""

    def __init__(
        self,
        dtypes: Optional[Union["torch.dtype", Dict[str, "torch.dtype"]]] = None,
        device: Optional[str] = None,
    ):
        self.dtypes = dtypes
        self.device = device

    def __call__(self, batch: "pyarrow.Table") -> Dict[str, List["torch.Tensor"]]:
        """Convert an Arrow batch to PyTorch tensors.

        Args:
            batch: PyArrow Table to convert

        Returns:
            A dictionary of column name to list of tensors
        """
        from ray.air._internal.torch_utils import (
            arrow_batch_to_tensors,
        )

        combine_chunks = self.device == "cpu"
        return arrow_batch_to_tensors(
            batch, dtypes=self.dtypes, device=None, combine_chunks=combine_chunks
        )


@DeveloperAPI
class DefaultNumpyCollateFn(NumpyBatchCollateFn):
    """Default collate function for converting Numpy batches to PyTorch tensors."""

    def __init__(
        self,
        dtypes: Optional[Union["torch.dtype", Dict[str, "torch.dtype"]]] = None,
        device: Optional[str] = None,
    ):
        self.dtypes = dtypes
        self.device = device

    def __call__(self, batch: Dict[str, np.ndarray]) -> Dict[str, List["torch.Tensor"]]:
        """Convert a Numpy batch to PyTorch tensors.

        Args:
            batch: The input dictionary of numpy arrays batch to collate

        Returns:
            A dictionary of column name to list of tensors
        """
        from ray.air._internal.torch_utils import (
            numpy_batch_to_torch_tensors,
        )

        return numpy_batch_to_torch_tensors(
            batch, dtypes=self.dtypes, device=self.device
        )


@DeveloperAPI
class DefaultPandasCollateFn(PandasBatchCollateFn):
    """Default collate function for converting Pandas batches to PyTorch tensors."""

    def __init__(
        self,
        dtypes: Optional[Union["torch.dtype", Dict[str, "torch.dtype"]]] = None,
        device: Optional[str] = None,
    ):
        self.dtypes = dtypes
        self.device = device

    def __call__(self, batch: "pandas.DataFrame") -> "torch.Tensor":
        """Convert a Pandas batch to PyTorch tensors.

        Args:
            batch: The input pandas.DataFrame batch to collate

        Returns:
            A PyTorch tensor containing the collated batch.
        """
        from ray.air._internal.torch_utils import (
            convert_pandas_to_torch_tensor,
        )

        return convert_pandas_to_torch_tensor(
            batch, dtypes=self.dtypes, device=self.device
        )


@DeveloperAPI
class DefaultFinalizeFn:
    """Default finalize function for moving PyTorch tensors to device."""

    def __init__(
        self,
        device: Optional[str] = None,
    ):
        """Initialize the finalize function.
        Args:
            device: Optional device to place tensors on
        """
        self.device = device

    def __call__(
        self, batch: Union[Dict[str, List["torch.Tensor"]], Any]
    ) -> Union[Dict[str, "torch.Tensor"], Any]:
        """Finalize the batch.
        Args:
            batch: Input batch to move to device. Can be:
                - Dictionary mapping column names to lists of tensors
                - Any other type supported by move_tensors_to_device
        Returns:
            Batch with tensors moved to the target device. Type matches input type:
            - If input is Dict[str, List[torch.Tensor]], returns Dict[str, torch.Tensor]
            - Otherwise returns the same type as input with tensors moved to device
        """
        from ray.air._internal.torch_utils import (
            move_tensors_to_device,
        )

        return move_tensors_to_device(batch, device=self.device)

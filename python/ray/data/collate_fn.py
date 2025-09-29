import abc
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Generic,
    List,
    Mapping,
    Optional,
    Tuple,
    TypeVar,
    Union,
)

import numpy as np

from ray.data.block import DataBatch
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    import pandas
    import pyarrow
    import torch

    from ray.data.dataset import CollatedData


DataBatchType = TypeVar("DataBatchType", bound=DataBatch)

TensorSequenceType = Union[
    List["torch.Tensor"],
    Tuple["torch.Tensor", ...],
]

TensorBatchType = Union[
    "torch.Tensor",
    TensorSequenceType,
    # For nested sequences of tensors, the inner sequence of tensors is combined during
    # GPU transfer in `move_tensors_to_device`.
    List[TensorSequenceType],
    Tuple[TensorSequenceType, ...],
    Mapping[str, "torch.Tensor"],
    # For mapping (e.g., dict) of keys to sequences of tensors, the sequence of tensors
    # is combined during GPU transfer in `move_tensors_to_device`.
    Mapping[str, TensorSequenceType],
]


def _is_tensor(batch: Any) -> bool:
    """Check if a batch is a single torch.Tensor."""
    import torch

    return isinstance(batch, torch.Tensor)


def _is_tensor_sequence(batch: Any) -> bool:
    """Check if a batch is a sequence of torch.Tensors.

    >>> import torch
    >>> _is_tensor_sequence(torch.ones(1))
    False
    >>> _is_tensor_sequence([torch.ones(1), torch.ones(1)])
    True
    >>> _is_tensor_sequence((torch.ones(1), torch.ones(1)))
    True
    >>> _is_tensor_sequence([torch.ones(1), 1])
    False
    """
    return isinstance(batch, (list, tuple)) and all(_is_tensor(t) for t in batch)


def _is_nested_tensor_sequence(batch: Any) -> bool:
    """Check if a batch is a sequence of sequences of torch.Tensors.

    Stops at one level of nesting.

    >>> import torch
    >>> _is_nested_tensor_sequence([torch.ones(1), torch.ones(1)])
    False
    >>> _is_nested_tensor_sequence(
    ...    ([torch.ones(1), torch.ones(1)], [torch.ones(1)])
    ... )
    True
    """
    return isinstance(batch, (list, tuple)) and all(
        _is_tensor_sequence(t) for t in batch
    )


def _is_tensor_mapping(batch: Any) -> bool:
    """Check if a batch is a mapping of keys to torch.Tensors.

    >>> import torch
    >>> _is_tensor_mapping({"a": torch.ones(1), "b": torch.ones(1)})
    True
    >>> _is_tensor_mapping({"a": torch.ones(1), "b": [torch.ones(1), torch.ones(1)]})
    False
    """
    return isinstance(batch, Mapping) and all(_is_tensor(v) for v in batch.values())


def _is_tensor_sequence_mapping(batch: Any) -> bool:
    """Check if a batch is a mapping of keys to sequences of torch.Tensors.

    >>> import torch
    >>> _is_tensor_sequence_mapping({"a": torch.ones(1), "b": torch.ones(1)})
    False
    >>> _is_tensor_sequence_mapping(
    ...    {"a": (torch.ones(1), torch.ones(1)), "b": [torch.ones(1), torch.ones(1)]}
    ... )
    True
    """
    return isinstance(batch, Mapping) and all(
        _is_tensor_sequence(v) for v in batch.values()
    )


@DeveloperAPI
def is_tensor_batch_type(batch: Any) -> bool:
    """Check if a batch matches any of the TensorBatchType variants.

    This function checks if the input batch is one of the following types:
    1. A single torch.Tensor
    2. A sequence of torch.Tensors
    3. A sequence of sequences of torch.Tensors
    4. A mapping (e.g., dict) of keys to torch.Tensors
    5. A mapping (e.g., dict) of keys to sequences of torch.Tensors

    Args:
        batch: The input batch to check. Can be any type.

    Returns:
        bool: True if the batch matches any TensorBatchType variant, False otherwise.
    """
    return (
        _is_tensor(batch)
        or _is_tensor_sequence(batch)
        or _is_nested_tensor_sequence(batch)
        or _is_tensor_mapping(batch)
        or _is_tensor_sequence_mapping(batch)
    )


TensorBatchReturnType = Union[
    "torch.Tensor",
    Tuple["torch.Tensor", ...],
    Dict[str, "torch.Tensor"],
]


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
    """Collate function that takes pyarrow.Table as the input batch type.
    Arrow tables with chunked arrays can be efficiently transferred to GPUs without
    combining the chunks with the `arrow_batch_to_tensors` utility function.
    See `DefaultCollateFn` for example.
    """

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
        pin_memory: bool = False,
    ):
        """Initialize the collate function.

        Args:
            dtypes: The torch dtype(s) for the created tensor(s); if None, the dtype
                will be inferred from the tensor data.
            device: The device on which the tensor should be placed. Can be a string
                (e.g. "cpu", "cuda:0") or a torch.device object.
            pin_memory: Whether to pin the memory of the created tensors.
        """
        import torch

        super().__init__()
        self.dtypes = dtypes
        if isinstance(device, str):
            self.device = torch.device(device)
        else:
            self.device = device
        self.pin_memory = pin_memory

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
            batch,
            dtypes=self.dtypes,
            combine_chunks=combine_chunks,
            pin_memory=self.pin_memory,
        )

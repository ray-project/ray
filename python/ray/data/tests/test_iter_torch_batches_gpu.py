from typing import Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest
import torch

import ray
from ray.air._internal.torch_utils import (
    arrow_batch_to_tensors,
    convert_ndarray_batch_to_torch_tensor_batch,
)
from ray.data.iterator import (
    ArrowBatchCollateFn,
    NumpyBatchCollateFn,
    PandasBatchCollateFn,
)
from ray.train.torch import get_device


class BaseArrowBatchCollateFn(ArrowBatchCollateFn):
    """Base class for Arrow batch collate functions that process and convert to tensors.

    This class provides common functionality for processing PyArrow tables and converting
    them to PyTorch tensors. It handles device placement and dtype conversion.

    Attributes:
        device: Optional device to place tensors on. Can be a string (e.g. "cpu", "cuda:0")
            or a torch.device object.
    """

    device: Optional[torch.device]

    def __init__(
        self,
        device: Optional[Union[str, torch.device]] = None,
    ) -> None:
        super().__init__()
        if isinstance(device, str):
            self.device = torch.device(device)
        else:
            self.device = device

    def _process_batch(self, batch: pa.Table) -> pa.Table:
        """Process the batch by adding 5 to the id column.

        Args:
            batch: Input PyArrow table containing an "id" column.

        Returns:
            A new PyArrow table with modified "id" column and original "value" column.
        """
        return pa.Table.from_arrays(
            [pa.compute.add(batch["id"], 5), batch["id"]],
            names=["id", "value"],
        )

    def _get_tensors(self, batch: pa.Table) -> Dict[str, torch.Tensor]:
        """Convert batch to tensors.

        Args:
            batch: Input PyArrow table to convert to tensors.

        Returns:
            Dictionary mapping column names to PyTorch tensors.
        """
        return arrow_batch_to_tensors(
            batch,
            combine_chunks=self.device.type == "cpu",
        )


class SingleTensorArrowBatchCollateFn(BaseArrowBatchCollateFn):
    """Collate function that returns only the id column as a tensor."""

    def __init__(
        self,
        device: Optional[Union[str, torch.device]] = None,
    ) -> None:
        super().__init__(device)

    def __call__(self, batch: pa.Table) -> torch.Tensor:
        """Return only the id column as a tensor."""
        assert isinstance(batch, pa.Table)
        modified_batch = self._process_batch(batch)
        return self._get_tensors(modified_batch)["id"]


class TupleArrowBatchCollateFn(BaseArrowBatchCollateFn):
    """Collate function that returns id and value as a tuple of tensors."""

    def __init__(
        self,
        device: Optional[Union[str, torch.device]] = None,
    ) -> None:
        super().__init__(device)

    def __call__(self, batch: pa.Table) -> Tuple[torch.Tensor, torch.Tensor]:
        """Return id and value as a tuple of tensors."""
        assert isinstance(batch, pa.Table)
        modified_batch = self._process_batch(batch)
        return (
            self._get_tensors(modified_batch)["id"],
            self._get_tensors(modified_batch)["value"],
        )


class DictArrowBatchCollateFn(BaseArrowBatchCollateFn):
    """Collate function that returns id and value as a dictionary of tensors."""

    def __init__(
        self,
        device: Optional[Union[str, torch.device]] = None,
    ) -> None:
        super().__init__(device)

    def __call__(self, batch: pa.Table) -> Dict[str, torch.Tensor]:
        """Return id and value as a dictionary of tensors."""
        assert isinstance(batch, pa.Table)
        modified_batch = self._process_batch(batch)
        return self._get_tensors(modified_batch)


class ListArrowBatchCollateFn(BaseArrowBatchCollateFn):
    """Collate function that returns id and value as a list of tensors."""

    def __init__(
        self,
        device: Optional[Union[str, torch.device]] = None,
    ) -> None:
        super().__init__(device)

    def __call__(self, batch: pa.Table) -> List[torch.Tensor]:
        """Return id and value as a list of tensors."""
        assert isinstance(batch, pa.Table)
        modified_batch = self._process_batch(batch)
        tensors = self._get_tensors(modified_batch)
        return [tensors["id"], tensors["value"]]


class BaseNumpyBatchCollateFn(NumpyBatchCollateFn):
    """Base class for Numpy batch collate functions that process and convert to tensors.

    This class provides common functionality for processing Numpy arrays and converting
    them to PyTorch tensors. It handles device placement and dtype conversion.

    Attributes:
        device: Optional device to place tensors on. Can be a string (e.g. "cpu", "cuda:0")
            or a torch.device object.
    """

    device: Optional[Union[str, torch.device]]

    def __init__(
        self,
        device: Optional[Union[str, torch.device]] = None,
    ) -> None:
        super().__init__()
        if isinstance(device, str):
            self.device = torch.device(device)
        else:
            self.device = device

    def _process_batch(self, batch: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
        """Process the batch by adding 5 to the id array.

        Args:
            batch: Input dictionary containing numpy arrays.

        Returns:
            A new dictionary with modified "id" array and original "value" array.
        """
        return {"id": batch["id"] + 5, "value": batch["id"]}

    def _get_tensors(self, batch: Dict[str, np.ndarray]) -> Dict[str, torch.Tensor]:
        """Convert batch to tensors.

        Args:
            batch: Input dictionary of numpy arrays to convert to tensors.

        Returns:
            Dictionary mapping column names to PyTorch tensors.
        """
        return convert_ndarray_batch_to_torch_tensor_batch(
            batch, dtypes=None, device=None
        )


class SingleTensorNumpyBatchCollateFn(BaseNumpyBatchCollateFn):
    """Collate function that returns only the id array as a tensor."""

    def __init__(
        self,
        device: Optional[Union[str, torch.device]] = None,
    ) -> None:
        super().__init__(device)

    def __call__(self, batch: Dict[str, np.ndarray]) -> torch.Tensor:
        """Return only the id array as a tensor."""
        assert isinstance(batch, dict)
        modified_batch = self._process_batch(batch)
        return self._get_tensors(modified_batch)["id"]


class TupleNumpyBatchCollateFn(BaseNumpyBatchCollateFn):
    """Collate function that returns id and value as a tuple of tensors."""

    def __init__(
        self,
        device: Optional[Union[str, torch.device]] = None,
    ) -> None:
        super().__init__(device)

    def __call__(
        self, batch: Dict[str, np.ndarray]
    ) -> Tuple[torch.Tensor, torch.Tensor]:
        """Return id and value as a tuple of tensors."""
        assert isinstance(batch, dict)
        modified_batch = self._process_batch(batch)
        tensors = self._get_tensors(modified_batch)
        return tensors["id"], tensors["value"]


class DictNumpyBatchCollateFn(BaseNumpyBatchCollateFn):
    """Collate function that returns id and value as a dictionary of tensors."""

    def __init__(
        self,
        device: Optional[Union[str, torch.device]] = None,
    ) -> None:
        super().__init__(device)

    def __call__(self, batch: Dict[str, np.ndarray]) -> Dict[str, torch.Tensor]:
        """Return id and value as a dictionary of tensors."""
        assert isinstance(batch, dict)
        modified_batch = self._process_batch(batch)
        return self._get_tensors(modified_batch)


class ListNumpyBatchCollateFn(BaseNumpyBatchCollateFn):
    """Collate function that returns id and value as a list of tensors."""

    def __init__(
        self,
        device: Optional[Union[str, torch.device]] = None,
    ) -> None:
        super().__init__(device)

    def __call__(self, batch: Dict[str, np.ndarray]) -> List[torch.Tensor]:
        """Return id and value as a list of tensors."""
        assert isinstance(batch, dict)
        modified_batch = self._process_batch(batch)
        tensors = self._get_tensors(modified_batch)
        return [tensors["id"], tensors["value"]]


class BasePandasBatchCollateFn(PandasBatchCollateFn):
    """Base class for Pandas batch collate functions that process and convert to tensors.

    This class provides common functionality for processing Pandas DataFrames and converting
    them to PyTorch tensors. It handles device placement and dtype conversion.

    Attributes:
        device: Optional device to place tensors on. Can be a string (e.g. "cpu", "cuda:0")
            or a torch.device object.
    """

    device: Optional[str]

    def __init__(
        self,
        device: Optional[Union[str, torch.device]] = None,
    ) -> None:
        super().__init__()
        if isinstance(device, str):
            self.device = torch.device(device)
        else:
            self.device = device

    def _process_batch(self, batch: pd.DataFrame) -> pd.DataFrame:
        """Process the batch by adding 5 to the id column.

        Args:
            batch: Input Pandas DataFrame.

        Returns:
            A new DataFrame with modified "id" column and original "value" column.
        """
        return pd.DataFrame({"id": batch["id"] + 5, "value": batch["id"]})

    def _get_tensors(self, batch: pd.DataFrame) -> Dict[str, torch.Tensor]:
        """Convert batch to tensors.

        Args:
            batch: Input Pandas DataFrame to convert to tensors.

        Returns:
            Dictionary mapping column names to PyTorch tensors.
        """
        return convert_ndarray_batch_to_torch_tensor_batch(
            batch.to_dict("series"), dtypes=None, device=None
        )


class SingleTensorPandasBatchCollateFn(BasePandasBatchCollateFn):
    """Collate function that returns only the id column as a tensor."""

    def __init__(
        self,
        device: Optional[Union[str, torch.device]] = None,
    ) -> None:
        super().__init__(device)

    def __call__(self, batch: pd.DataFrame) -> torch.Tensor:
        """Return only the id column as a tensor."""
        modified_batch = self._process_batch(batch)
        return self._get_tensors(modified_batch)["id"]


class TuplePandasBatchCollateFn(BasePandasBatchCollateFn):
    """Collate function that returns id and value as a tuple of tensors."""

    def __init__(
        self,
        device: Optional[Union[str, torch.device]] = None,
    ) -> None:
        super().__init__(device)

    def __call__(self, batch: pd.DataFrame) -> Tuple[torch.Tensor, torch.Tensor]:
        """Return id and value as a tuple of tensors."""
        assert isinstance(batch, pd.DataFrame)
        modified_batch = self._process_batch(batch)
        tensors = self._get_tensors(modified_batch)
        return tensors["id"], tensors["value"]

class DictPandasBatchCollateFn(BasePandasBatchCollateFn):
    """Collate function that returns id and value as a dictionary of tensors."""

    def __init__(
        self,
        device: Optional[Union[str, torch.device]] = None,
    ) -> None:
        super().__init__(device)

    def __call__(self, batch: pd.DataFrame) -> Dict[str, torch.Tensor]:
        """Return id and value as a dictionary of tensors."""
        assert isinstance(batch, pd.DataFrame)
        modified_batch = self._process_batch(batch)
        return self._get_tensors(modified_batch)

class ListPandasBatchCollateFn(BasePandasBatchCollateFn):
    """Collate function that returns id and value as a list of tensors."""

    def __init__(
        self,
        device: Optional[Union[str, torch.device]] = None,
    ) -> None:
        super().__init__(device)

    def __call__(self, batch: pd.DataFrame) -> List[torch.Tensor]:
        """Return id and value as a list of tensors."""
        assert isinstance(batch, pd.DataFrame)
        modified_batch = self._process_batch(batch)
        tensors = self._get_tensors(modified_batch)
        return [tensors["id"], tensors["value"]]


@pytest.fixture
def custom_collate_fns():
    """Fixture that provides both Arrow and Numpy custom collate functions."""
    def _create_collate_fns(device):
        return {
            "arrow": {
                "single": SingleTensorArrowBatchCollateFn(device=device),
                "tuple": TupleArrowBatchCollateFn(device=device),
                "dict": DictArrowBatchCollateFn(device=device),
                "list": ListArrowBatchCollateFn(device=device),
            },
            "numpy": {
                "single": SingleTensorNumpyBatchCollateFn(device=device),
                "tuple": TupleNumpyBatchCollateFn(device=device),
                "dict": DictNumpyBatchCollateFn(device=device),
                "list": ListNumpyBatchCollateFn(device=device),
            },
            "pandas": {
                "single": SingleTensorPandasBatchCollateFn(device=device),
                "tuple": TuplePandasBatchCollateFn(device=device),
                "dict": DictPandasBatchCollateFn(device=device),
                "list": ListPandasBatchCollateFn(device=device),
            },
        }
    return _create_collate_fns


@pytest.mark.parametrize(
    "collate_type,return_type,device",
    [
        ("arrow", "single", "cpu"),
        ("arrow", "single", "cuda"),
        ("arrow", "tuple", "cpu"),
        ("arrow", "tuple", "cuda"),
        ("arrow", "dict", "cpu"),
        ("arrow", "dict", "cuda"),
        ("arrow", "list", "cpu"),
        ("arrow", "list", "cuda"),
        ("numpy", "single", "cpu"),
        ("numpy", "single", "cuda"),
        ("numpy", "tuple", "cpu"),
        ("numpy", "tuple", "cuda"),
        ("numpy", "dict", "cpu"),
        ("numpy", "dict", "cuda"),
        ("numpy", "list", "cpu"),
        ("numpy", "list", "cuda"),
        ("pandas", "single", "cpu"),
        ("pandas", "single", "cuda"),
        ("pandas", "tuple", "cpu"),
        ("pandas", "tuple", "cuda"),
        ("pandas", "dict", "cpu"),
        ("pandas", "dict", "cuda"),
        ("pandas", "list", "cpu"),
        ("pandas", "list", "cuda"),
    ],
)
def test_custom_batch_collate_fn(
    ray_start_regular_shared, custom_collate_fns, collate_type, return_type, device
):
    """Tests that custom batch collate functions can be used to modify
    the batch before it is converted to a PyTorch tensor."""
    # Skip GPU tests if CUDA is not available
    if device != "cpu" and not torch.cuda.is_available():
        pytest.skip("CUDA is not available")

    # Get the actual device to use
    if device == "cuda":
        device = str(get_device())

    ds = ray.data.range(5)
    it = ds.iterator()

    collate_fns = custom_collate_fns(device)
    collate_fn = (
        collate_fns[collate_type][return_type]
        if return_type
        else collate_fns[collate_type]
    )

    for batch in it.iter_torch_batches(collate_fn=collate_fn):
        if return_type == "single":
            assert isinstance(batch, torch.Tensor)
            assert batch.tolist() == list(range(5, 10))
            assert str(batch.device) == device
        elif return_type == "dict":
            assert isinstance(batch, dict)
            assert batch["id"].tolist() == list(range(5, 10))
            assert batch["value"].tolist() == list(range(5))
            assert str(batch["id"].device) == device
            assert str(batch["value"].device) == device
        else:  # tuple or list
            assert isinstance(batch, torch.Tensor)
            # For tuple/list return types, tensors are concatenated
            # First 5 values: modified id values [5,6,7,8,9]
            # Last 5 values: original values [0,1,2,3,4]
            assert batch.tolist() == list(range(5, 10)) + list(range(5))
            assert str(batch.device) == device


if __name__ == "__main__":
    import sys

    if sys.version_info >= (3, 12):
        # Skip this test for Python 3.12+ due to to incompatibility tensorflow
        sys.exit(0)

    sys.exit(pytest.main(["-v", __file__]))

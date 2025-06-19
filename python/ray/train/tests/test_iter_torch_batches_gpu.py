from typing import Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest
import torch

import ray
import ray.train.torch
from ray.air._internal.torch_utils import (
    arrow_batch_to_tensors,
    convert_ndarray_batch_to_torch_tensor_batch,
)
from ray.data.iterator import (
    ArrowBatchCollateFn,
    NumpyBatchCollateFn,
    PandasBatchCollateFn,
)


@pytest.fixture(scope="module")
def ray_start_4_cpus_1_gpu():
    address_info = ray.init(num_cpus=4, num_gpus=1)
    yield address_info
    ray.shutdown()


def _chunk_table_in_half(table: pa.Table) -> pa.Table:
    num_rows = table.num_rows
    mid = num_rows // 2

    new_columns = []

    for col in table.itercolumns():
        # Slice the column in two halves
        first_half = col.slice(0, mid)
        second_half = col.slice(mid)

        # Create a chunked array with two chunks
        chunked_array = pa.chunked_array([first_half, second_half], type=col.type)
        new_columns.append(chunked_array)

    # Create a new table with the same schema but chunked columns
    return pa.Table.from_arrays(new_columns, schema=table.schema)


class SingleTensorArrowBatchCollateFn(ArrowBatchCollateFn):
    """Collate function that returns only the id column as a tensor."""

    def __call__(self, batch: pa.Table) -> torch.Tensor:
        """Return only the id column as a tensor."""
        assert isinstance(batch, pa.Table)
        tensor_dict = arrow_batch_to_tensors(batch, combine_chunks=True)
        return tensor_dict["id"]


class TupleArrowBatchCollateFn(ArrowBatchCollateFn):
    """Collate function that returns id and value as a tuple of tensors."""

    def __call__(self, batch: pa.Table) -> Tuple[torch.Tensor, torch.Tensor]:
        """Return id and value as a tuple of tensors."""
        assert isinstance(batch, pa.Table)
        tensor_dict = arrow_batch_to_tensors(batch, combine_chunks=True)
        return tensor_dict["id"], tensor_dict["value"]


class ListArrowBatchCollateFn(TupleArrowBatchCollateFn):
    """Collate function that returns id and value as a list of tensors."""

    def __call__(self, batch: pa.Table) -> List[torch.Tensor]:
        return list(super().__call__(batch))


class DictArrowBatchCollateFn(ArrowBatchCollateFn):
    """Collate function that returns id and value as a dictionary of tensors."""

    def __call__(self, batch: pa.Table) -> Dict[str, torch.Tensor]:
        """Return id and value as a dictionary of tensors."""
        assert isinstance(batch, pa.Table)
        return arrow_batch_to_tensors(batch, combine_chunks=True)


class ChunkedDictArrowBatchCollateFn(ArrowBatchCollateFn):
    """Collate function that returns id and value as a dictionary of chunked tensors."""

    def __call__(self, batch: pa.Table) -> Dict[str, List[torch.Tensor]]:
        assert isinstance(batch, pa.Table)
        modified_batch = _chunk_table_in_half(batch)
        return arrow_batch_to_tensors(modified_batch, combine_chunks=False)


class SingleTensorNumpyBatchCollateFn(NumpyBatchCollateFn):
    """Collate function that returns only the id array as a tensor."""

    def __call__(self, batch: Dict[str, np.ndarray]) -> torch.Tensor:
        """Return only the id array as a tensor."""
        assert isinstance(batch, dict)
        tensor_dict = convert_ndarray_batch_to_torch_tensor_batch(batch)
        return tensor_dict["id"]


class TupleNumpyBatchCollateFn(NumpyBatchCollateFn):
    """Collate function that returns id and value as a tuple of tensors."""

    def __call__(
        self, batch: Dict[str, np.ndarray]
    ) -> Tuple[torch.Tensor, torch.Tensor]:
        assert isinstance(batch, dict)
        tensor_dict = convert_ndarray_batch_to_torch_tensor_batch(batch)
        return tensor_dict["id"], tensor_dict["value"]


class ListNumpyBatchCollateFn(TupleNumpyBatchCollateFn):
    """Collate function that returns id and value as a list of tensors."""

    def __call__(self, batch: Dict[str, np.ndarray]) -> List[torch.Tensor]:
        return list(super().__call__(batch))


class DictNumpyBatchCollateFn(NumpyBatchCollateFn):
    """Collate function that returns id and value as a dictionary of tensors."""

    def __call__(self, batch: Dict[str, np.ndarray]) -> Dict[str, torch.Tensor]:
        assert isinstance(batch, dict)
        return convert_ndarray_batch_to_torch_tensor_batch(batch)


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


class SingleTensorPandasBatchCollateFn(PandasBatchCollateFn):
    """Collate function that returns only the id column as a tensor."""

    def __call__(self, batch: pd.DataFrame) -> torch.Tensor:
        tensor_dict = convert_ndarray_batch_to_torch_tensor_batch(
            batch.to_dict("series")
        )
        return tensor_dict["id"]


class TuplePandasBatchCollateFn(PandasBatchCollateFn):
    """Collate function that returns id and value as a tuple of tensors."""

    def __call__(self, batch: pd.DataFrame) -> Tuple[torch.Tensor, torch.Tensor]:
        tensor_dict = convert_ndarray_batch_to_torch_tensor_batch(
            batch.to_dict("series")
        )
        return tensor_dict["id"], tensor_dict["value"]


class ListPandasBatchCollateFn(TuplePandasBatchCollateFn):
    """Collate function that returns id and value as a list of tensors."""

    def __call__(self, batch: pd.DataFrame) -> List[torch.Tensor]:
        return list(super().__call__(batch))


class DictPandasBatchCollateFn(PandasBatchCollateFn):
    """Collate function that returns id and value as a dictionary of tensors."""

    def __call__(self, batch: pd.DataFrame) -> Dict[str, torch.Tensor]:
        return convert_ndarray_batch_to_torch_tensor_batch(batch.to_dict("series"))


@pytest.fixture
def collate_fn_map():
    """Fixture that provides Arrow, Numpy, Pandas custom collate functions."""

    return {
        "arrow": {
            "default": None,
            "single": SingleTensorArrowBatchCollateFn(),
            "tuple": TupleArrowBatchCollateFn(),
            "list": ListArrowBatchCollateFn(),
            "dict": DictArrowBatchCollateFn(),
            "chunked_dict": ChunkedDictArrowBatchCollateFn(),
        },
        "numpy": {
            "single": SingleTensorNumpyBatchCollateFn(),
            "tuple": TupleNumpyBatchCollateFn(),
            "dict": DictNumpyBatchCollateFn(),
            "list": ListNumpyBatchCollateFn(),
        },
        "pandas": {
            "single": SingleTensorPandasBatchCollateFn(),
            "tuple": TuplePandasBatchCollateFn(),
            "dict": DictPandasBatchCollateFn(),
            "list": ListPandasBatchCollateFn(),
        },
    }


@pytest.mark.parametrize("collate_batch_type", ["arrow", "numpy", "pandas"])
@pytest.mark.parametrize(
    "return_type", ["single", "tuple", "dict", "list", "chunked_dict", "default"]
)
@pytest.mark.parametrize("device", ["cpu", "cuda:0"])
@pytest.mark.parametrize("pin_memory", [True, False])
def test_custom_batch_collate_fn(
    ray_start_4_cpus_1_gpu,
    monkeypatch,
    collate_batch_type,
    return_type,
    device,
    collate_fn_map,
    pin_memory,
):
    """Tests that custom batch collate functions can be used to modify
    the batch before it is converted to a PyTorch tensor.

    Note that the collate_fn doesn't move the tensors to the device --
    that happens in the iterator (finalize_fn).
    """
    # Skip GPU tests if CUDA is not available
    if device == "cuda:0" and not torch.cuda.is_available():
        pytest.skip("CUDA is not available")

    # Skip pin_memory tests if CUDA is not available
    if pin_memory and not torch.cuda.is_available():
        pytest.skip("pin_memory is set to True, but CUDA is not available.")

    # Skip tests if pin_memory is set to True and the collate function is not the
    # DefaultCollateFn.
    if pin_memory and not (collate_batch_type == "arrow" and return_type == "default"):
        pytest.skip(
            "pin_memory is set to True, but the collate function is not the DefaultCollateFn."
        )

    collate_fn = collate_fn_map[collate_batch_type].get(return_type)
    if collate_fn is None:
        pytest.skip(
            f"Collate function not found for ({collate_batch_type}, {return_type})"
        )

    # Set the device that's returned by device="auto" -> get_device()
    # This is used in `finalize_fn` to move the tensors to the correct device.
    device = torch.device(device)
    monkeypatch.setattr(ray.train.torch, "get_device", lambda: device)

    ds = ray.data.from_items(
        [{"id": i + 5, "value": i} for i in range(5)],
    )
    it = ds.iterator()

    for batch in it.iter_torch_batches(collate_fn=collate_fn, pin_memory=pin_memory):
        if return_type == "single":
            assert isinstance(batch, torch.Tensor)
            assert sorted(batch.tolist()) == list(range(5, 10))
            assert batch.device == device
            if pin_memory and device.type == "cpu":
                assert batch.is_pinned()
        elif return_type == "dict" or return_type == "chunked_dict":
            # Chunked dicts get concatenated to single Tensors on the device,
            # so the assertions are shared with the dict case.
            assert isinstance(batch, dict)
            assert sorted(batch["id"].tolist()) == list(range(5, 10))
            assert sorted(batch["value"].tolist()) == list(range(5))
            assert batch["id"].device == device
            assert batch["value"].device == device
            if pin_memory and device.type == "cpu":
                assert batch["id"].is_pinned()
                assert batch["value"].is_pinned()
        else:  # tuple or list
            assert isinstance(batch, (tuple, list))
            assert len(batch) == 2
            assert sorted(batch[0].tolist()) == list(range(5, 10))
            assert sorted(batch[1].tolist()) == list(range(5))
            assert batch[0].device == device
            assert batch[1].device == device
            if pin_memory and device.type == "cpu":
                assert batch[0].is_pinned()
                assert batch[1].is_pinned()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))

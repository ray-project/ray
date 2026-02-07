import sys

import numpy as np
import pandas as pd
import pytest
import torch

from ray.data.util.torch_utils import (
    concat_tensors_to_device,
    convert_pandas_to_torch_tensor,
    move_tensors_to_device,
)
from ray.util.debug import _test_some_code_for_memory_leaks

data_batch = pd.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6]})


class TestConvertPandasToTorch:
    def test_invalid_args(self):
        with pytest.raises(TypeError):
            convert_pandas_to_torch_tensor(
                data_batch, columns=["A", "B"], column_dtypes=[torch.float, torch.float]
            )

    def test_single_tensor(self):
        tensor = convert_pandas_to_torch_tensor(data_batch)
        assert tensor.size() == (len(data_batch), len(data_batch.columns))
        assert np.array_equal(tensor.numpy(), data_batch.to_numpy())

    def test_single_tensor_dtype(self):
        tensor = convert_pandas_to_torch_tensor(data_batch, column_dtypes=torch.float)
        assert tensor.size() == (len(data_batch), len(data_batch.columns))
        assert tensor.dtype == torch.float
        assert np.array_equal(tensor.numpy(), data_batch.to_numpy())

    def test_single_tensor_columns(self):
        tensor = convert_pandas_to_torch_tensor(data_batch, columns=["A"])
        assert tensor.size() == (len(data_batch), len(data_batch.columns) - 1)
        assert np.array_equal(tensor.numpy(), data_batch[["A"]].to_numpy())

    def test_multi_input(self):
        tensors = convert_pandas_to_torch_tensor(data_batch, columns=[["A"], ["B"]])
        assert len(tensors) == 2

        for i in range(len(tensors)):
            tensor = tensors[i]
            assert tensor.size() == (len(data_batch), 1)
            assert np.array_equal(
                tensor.numpy(), data_batch[[data_batch.columns[i]]].to_numpy()
            )

    def test_multi_input_single_dtype(self):
        tensors = convert_pandas_to_torch_tensor(
            data_batch, columns=[["A"], ["B"]], column_dtypes=torch.float
        )
        assert len(tensors) == 2

        for i in range(len(tensors)):
            tensor = tensors[i]
            assert tensor.dtype == torch.float
            assert tensor.size() == (len(data_batch), 1)
            assert np.array_equal(
                tensor.numpy(), data_batch[[data_batch.columns[i]]].to_numpy()
            )

    def test_multi_input_multi_dtype(self):
        column_dtypes = [torch.long, torch.float]
        tensors = convert_pandas_to_torch_tensor(
            data_batch, columns=[["A"], ["B"]], column_dtypes=column_dtypes
        )
        assert len(tensors) == 2

        for i in range(len(tensors)):
            tensor = tensors[i]
            assert tensor.dtype == column_dtypes[i]
            assert tensor.size() == (len(data_batch), 1)
            assert np.array_equal(
                tensor.numpy(), data_batch[[data_batch.columns[i]]].to_numpy()
            )

    def test_tensor_column_no_memory_leak(self):
        # Test that converting a Pandas DataFrame containing an object-dtyped tensor
        # column (e.g. post-casting from extension type) doesn't leak memory. Casting
        # these tensors directly with torch.as_tensor() currently leaks memory; see
        # https://github.com/ray-project/ray/issues/30629#issuecomment-1330954556
        def code():
            col = np.empty(1000, dtype=object)
            col[:] = [np.ones((100, 100)) for _ in range(1000)]
            df = pd.DataFrame({"a": col})
            convert_pandas_to_torch_tensor(
                df, columns=[["a"]], column_dtypes=[torch.int]
            )

        suspicious_stats = _test_some_code_for_memory_leaks(
            desc="Testing convert_pandas_to_torch_tensor for memory leaks.",
            init=None,
            code=code,
            repeats=10,
        )
        assert not suspicious_stats


def test_move_tensors_to_device():
    """Test that move_tensors_to_device moves and concatenates tensors correctly."""
    device = torch.device("cpu")

    # torch.Tensor
    batch = torch.ones(1)
    t = move_tensors_to_device(batch, device)
    assert torch.equal(t, torch.ones(1))

    # Tuple[torch.Tensor]
    batch = (torch.ones(1), torch.ones(1))
    t1, t2 = move_tensors_to_device(batch, device)
    assert torch.equal(t1, torch.ones(1))
    assert torch.equal(t2, torch.ones(1))

    # List[torch.Tensor]
    batch = [torch.ones(1), torch.ones(1)]
    t1, t2 = move_tensors_to_device(batch, device)
    assert torch.equal(t1, torch.ones(1))
    assert torch.equal(t2, torch.ones(1))

    # List/Tuple[List/Tuple[torch.Tensor]]
    batch = [(torch.ones(1), torch.ones(1)), [torch.ones(1)]]
    t1, t2 = move_tensors_to_device(batch, device)
    assert torch.equal(t1, torch.ones(2))
    assert torch.equal(t2, torch.ones(1))

    # Dict[str, torch.Tensor]
    batch = {"a": torch.ones(1), "b": torch.ones(1)}
    out = move_tensors_to_device(batch, device)
    assert torch.equal(out["a"], torch.ones(1))
    assert torch.equal(out["b"], torch.ones(1))

    # Dict[str, List/Tuple[torch.Tensor]]
    batch = {"a": [torch.ones(1)], "b": (torch.ones(1), torch.ones(1))}
    out = move_tensors_to_device(batch, device)
    assert torch.equal(out["a"], torch.ones(1))
    assert torch.equal(out["b"], torch.ones(2))


def test_move_invalid_batch_type():
    """Test that move_tensors_to_device raises an error for invalid batch types."""
    device = torch.device("cpu")
    with pytest.raises(ValueError, match="Invalid input type"):
        move_tensors_to_device("invalid", device)

    with pytest.raises(ValueError, match="Invalid input type: list[int | list[int]]*"):
        move_tensors_to_device([1, 2, [3]], device)

    with pytest.raises(
        ValueError, match="Invalid input type: list[Tensor | list[Tensor]]*"
    ):
        move_tensors_to_device([torch.ones(1), [torch.ones(1)]], device)

    with pytest.raises(
        ValueError, match="Invalid input type: dict[str, Tensor | list[Tensor]]*"
    ):
        move_tensors_to_device({"a": torch.ones(1), "b": [torch.ones(1)]}, device)


def test_concat_tensors_to_device_no_copy():
    """Test concat_tensors_to_device when copy is not needed.

    This tests the optimization path where there is only one tensor
    and its device already matches the target device, so the tensor
    is returned directly without copying.
    """
    # Test case 1: Single tensor with device=None (no copy needed)
    tensor = torch.tensor([1.0, 2.0, 3.0])
    result = concat_tensors_to_device([tensor], device=None)
    # Should return the same tensor object, not a copy
    assert result is tensor
    assert torch.equal(result, tensor)

    # Test case 2: Single tensor on CPU with device="cpu" (no copy needed)
    tensor = torch.tensor([4.0, 5.0, 6.0], device="cpu")
    result = concat_tensors_to_device([tensor], device="cpu")
    # Should return the same tensor object, not a copy
    assert result is tensor
    assert torch.equal(result, tensor)

    # Test case 3: Single tensor on CPU with device=torch.device("cpu") (no copy needed)
    tensor = torch.tensor([7.0, 8.0, 9.0], device="cpu")
    result = concat_tensors_to_device([tensor], device=torch.device("cpu"))
    # Should return the same tensor object, not a copy
    assert result is tensor
    assert torch.equal(result, tensor)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))

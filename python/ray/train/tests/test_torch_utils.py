import numpy as np
import pandas as pd
import pytest
import torch

from ray.air._internal.torch_utils import (
    convert_pandas_to_torch_tensor,
    load_torch_model,
    contains_tensor,
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


torch_module = torch.nn.Linear(1, 1)


class TestLoadTorchModel:
    def test_load_module(self):
        assert load_torch_model(torch_module) == torch_module

    def test_load_state_dict(self):
        state_dict = torch_module.state_dict()
        model_definition = torch.nn.Linear(1, 1)
        assert model_definition.state_dict() != state_dict

        assert load_torch_model(state_dict, model_definition).state_dict() == state_dict

    def test_load_state_dict_fail(self):
        with pytest.raises(ValueError):
            # model_definition is required to load state dict.
            load_torch_model(torch_module.state_dict())


def test_contains_tensor():
    t = torch.tensor([0])
    assert contains_tensor(t)
    assert contains_tensor([1, 2, 3, t, 5, 6])
    assert contains_tensor([1, 2, 3, {"dict": t}, 5, 6])
    assert contains_tensor({"outer": [1, 2, 3, {"dict": t}, 5, 6]})
    assert contains_tensor({t: [1, 2, 3, {"dict": 2}, 5, 6]})
    assert not contains_tensor([4, 5, 6])


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))

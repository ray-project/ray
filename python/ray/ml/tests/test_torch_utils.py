import pytest

import numpy as np
import pandas as pd
import torch

from ray.ml.utils.torch_utils import convert_pandas_to_torch_tensor

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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))

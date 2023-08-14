import unittest

import numpy as np
import torch.cuda

import ray
from ray.rllib.utils.torch_utils import (
    convert_to_torch_tensor,
    copy_torch_tensors,
)


class TestTorchUtils(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_convert_to_torch_tensor(self):
        # Tests whether convert_to_torch_tensor works as expected

        # Test None
        self.assertTrue(convert_to_torch_tensor(None) is None)

        # Test single array
        array = np.array([1, 2, 3])
        tensor = torch.from_numpy(array)
        self.assertTrue(all(convert_to_torch_tensor(array) == tensor))

        # Test torch tensor
        self.assertTrue(convert_to_torch_tensor(tensor) is tensor)

        # Test conversion to 32-bit float
        tensor_2 = torch.tensor([1.0, 2.0, 3.0], dtype=torch.float64)
        self.assertTrue(convert_to_torch_tensor(tensor_2).dtype is torch.float32)

        # Test nested structure with objects tested above
        converted = convert_to_torch_tensor(
            {"a": (array, tensor), "b": tensor_2, "c": None}
        )
        self.assertTrue(all(convert_to_torch_tensor(converted["a"][0]) == tensor))
        self.assertTrue(converted["a"][1] is tensor)
        self.assertTrue(converted["b"].dtype is torch.float32)
        self.assertTrue(converted["c"] is None)

    def test_copy_torch_tensors(self):
        array = np.array([1, 2, 3], dtype=np.float32)
        device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")
        tensor = torch.from_numpy(array).to(device)
        tensor_2 = torch.tensor([1.0, 2.0, 3.0], dtype=torch.float64).to(device)

        # Test single tensor
        copied_tensor = copy_torch_tensors(tensor, device)
        self.assertTrue(copied_tensor.device == device)
        self.assertNotEqual(id(copied_tensor), id(tensor))
        self.assertTrue(all(copied_tensor == tensor))

        # check that dtypes aren't modified
        copied_tensor_2 = copy_torch_tensors(tensor_2, device)
        self.assertTrue(copied_tensor_2.dtype == tensor_2.dtype)
        self.assertFalse(copied_tensor_2.dtype == torch.float32)

        # Test nested structure can be converted
        nested_structure = {"a": tensor, "b": tensor_2, "c": 1}
        copied_nested_structure = copy_torch_tensors(nested_structure, device)
        self.assertTrue(copied_nested_structure["a"].device == device)
        self.assertTrue(copied_nested_structure["b"].device == device)
        self.assertTrue(copied_nested_structure["c"] == 1)
        self.assertNotEqual(id(copied_nested_structure["a"]), id(tensor))
        self.assertNotEqual(id(copied_nested_structure["b"]), id(tensor_2))
        self.assertTrue(all(copied_nested_structure["a"] == tensor))
        self.assertTrue(all(copied_nested_structure["b"] == tensor_2))

        # if gpu is available test moving tensor from cpu to gpu and vice versa
        if torch.cuda.is_available():
            tensor = torch.from_numpy(array).to("cpu")
            copied_tensor = copy_torch_tensors(tensor, "cuda:0")
            self.assertFalse(copied_tensor.device == torch.device("cpu"))
            self.assertTrue(copied_tensor.device == torch.device("cuda:0"))
            self.assertNotEqual(id(copied_tensor), id(tensor))
            self.assertTrue(
                all(copied_tensor.detach().cpu().numpy() == tensor.detach().numpy())
            )

            tensor = torch.from_numpy(array).to("cuda:0")
            copied_tensor = copy_torch_tensors(tensor, "cpu")
            self.assertFalse(copied_tensor.device == torch.device("cuda:0"))
            self.assertTrue(copied_tensor.device == torch.device("cpu"))
            self.assertNotEqual(id(copied_tensor), id(tensor))
            self.assertTrue(
                all(copied_tensor.detach().numpy() == tensor.detach().cpu().numpy())
            )


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))

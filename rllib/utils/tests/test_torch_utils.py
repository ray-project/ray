import unittest

import numpy as np
import torch.cuda

import ray
from ray.rllib.utils.torch_utils import convert_to_torch_tensor


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


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))

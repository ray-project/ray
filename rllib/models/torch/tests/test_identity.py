import unittest

import torch

from ray.rllib.models.configs.identity import IdentityConfig
from ray.rllib.models.specs.specs_dict import ModelSpec
from ray.rllib.models.specs.specs_torch import TorchTensorSpec
from ray.rllib.utils.nested_dict import NestedDict


class TestConfig(unittest.TestCase):
    def test_build(self):
        """Test building with the default config"""
        input_spec = ModelSpec({"bork": TorchTensorSpec("a, b, c", c=3)})
        c = IdentityConfig()
        c.build(input_spec)

    def test_identity(self):
        """Test the default config/model _forward implementation"""
        input_spec = ModelSpec({"bork": TorchTensorSpec("a, b, c", c=3)})
        c = IdentityConfig()
        m = c.build(input_spec)
        inputs = NestedDict({"bork": torch.rand((2, 4, 3))})
        self.assertEqual(m(inputs), inputs)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))

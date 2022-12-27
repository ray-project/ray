import unittest

import torch

from ray.rllib.models.configs.encoder import VectorEncoderConfig
from ray.rllib.models.specs.specs_dict import SpecDict
from ray.rllib.models.specs.specs_torch import TorchTensorSpec
from ray.rllib.utils.nested_dict import NestedDict


class TestConfig(unittest.TestCase):
    def test_error_no_feature_dim(self):
        """Ensure we error out if we don't know the input dim"""
        input_spec = SpecDict({"bork": TorchTensorSpec("a, b, c")})
        c = VectorEncoderConfig()
        with self.assertRaises(AssertionError):
            c.build(input_spec)

    def test_default_build(self):
        """Test building with the default config"""
        input_spec = SpecDict({"bork": TorchTensorSpec("a, b, c", c=3)})
        c = VectorEncoderConfig()
        c.build(input_spec)

    def test_nonlinear_final_build(self):
        input_spec = SpecDict({"bork": TorchTensorSpec("a, b, c", c=3)})
        c = VectorEncoderConfig(final_activation="relu")
        c.build(input_spec)

    def test_default_forward(self):
        """Test the default config/model _forward implementation"""
        input_spec = SpecDict({"bork": TorchTensorSpec("a, b, c", c=3)})
        c = VectorEncoderConfig()
        m = c.build(input_spec)
        inputs = NestedDict({"bork": torch.rand((2, 4, 3))})
        outputs, _ = m.unroll(inputs, NestedDict())
        self.assertEqual(outputs[c.output_key].shape[-1], c.hidden_layer_sizes[-1])
        self.assertEqual(outputs[c.output_key].shape[:-1], (2, 4))

    def test_two_inputs_forward(self):
        """Test the default model when we have two items in the input_spec.
        These two items will be concatenated and fed thru the mlp."""
        """Test the default config/model _forward implementation"""
        input_spec = SpecDict(
            {
                "bork": TorchTensorSpec("a, b, c", c=3),
                "dork": TorchTensorSpec("x, y, z", z=5),
            }
        )
        c = VectorEncoderConfig()
        m = c.build(input_spec)
        self.assertEqual(m.net[0].in_features, 8)
        inputs = NestedDict(
            {"bork": torch.rand((2, 4, 3)), "dork": torch.rand((2, 4, 5))}
        )
        outputs, _ = m.unroll(inputs, NestedDict())
        self.assertEqual(outputs[c.output_key].shape[-1], c.hidden_layer_sizes[-1])
        self.assertEqual(outputs[c.output_key].shape[:-1], (2, 4))

    def test_deep_build(self):
        input_spec = SpecDict({"bork": TorchTensorSpec("a, b, c", c=3)})
        c = VectorEncoderConfig()
        c.build(input_spec)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))

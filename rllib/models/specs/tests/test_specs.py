import unittest
import torch
import numpy as np

from ray.rllib.models.specs.specs_torch import TorchSpecs
from ray.rllib.models.specs.specs_np import NPSpecs

SPEC_CLASSES = {
    "torch": TorchSpecs,
    "np": NPSpecs
}

DOUBLE_TYPE = {
    "torch": torch.float64,
    "np": np.float64
}

FLOAT_TYPE = {
    "torch": torch.float32,
    "np": np.float32
}
    
class TestSpecs(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        pass
    

    def test_tensor_spec_2d(self):

        b, h = 2, 3

        for fw in SPEC_CLASSES.keys():
            spec_class = SPEC_CLASSES[fw]
            double_type = DOUBLE_TYPE[fw]
            float_type = FLOAT_TYPE[fw]

            tensor_2d = spec_class("b h", b=b, h=h, dtype=double_type).sample()

            matching_specs = [
                spec_class("b h"),
                spec_class("b h", h=h),
                spec_class("b h", h=h, b=b),
                spec_class("b h", b=b, dtype=double_type),
            ]

            # check if get_shape returns a tuple of ints
            shape = matching_specs[0].get_shape(tensor_2d)
            self.assertIsInstance(shape, tuple)
            self.assertTrue(all(isinstance(x, int) for x in shape))

            # check matching
            for spec in matching_specs:
                spec.validate(tensor_2d)

            non_matching_specs = [
                spec_class("b"),
                spec_class("b h1 h2"),
                spec_class("b h", h=h + 1),
                spec_class("b h", dtype=float_type),
            ]

            for spec in non_matching_specs:
                self.assertRaises(ValueError, lambda: spec.validate(tensor_2d))

            # non unique dimensions
            self.assertRaises(ValueError, lambda: spec_class("b b"))
            # unknown dimensions
            self.assertRaises(ValueError, lambda: spec_class("b h", b=1, h=2, c=3))
            self.assertRaises(ValueError, lambda: spec_class("b1", b2=1))
            # zero dimensions
            self.assertRaises(ValueError, lambda: spec_class("b h", b=1, h=0))
            # non-integer dimension
            self.assertRaises(ValueError, lambda: spec_class("b h", b=1, h="h"))


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))

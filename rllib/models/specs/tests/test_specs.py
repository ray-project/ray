import unittest
import torch

from ray.rllib.models.specs.specs_torch import TorchSpecs


class TestSpecs(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        pass

    def test_1(self):

        b, h = 2, 3
        tensor_1 = torch.randn(b, h).double()


        matching_specs = [
            TorchSpecs("b h"),
            TorchSpecs("b h", h=h),
            TorchSpecs("b h", h=h, b=b),
            TorchSpecs("b h", b=b, dtype=torch.double),
        ]

        # check if get_shape returns a tuple of ints
        shape = matching_specs[0].get_shape(tensor_1)
        self.assertIsInstance(shape, tuple)
        self.assertTrue(all(isinstance(x, int) for x in shape))

        # check matching
        for spec in matching_specs:
            spec.validate(tensor_1)

        non_matching_specs = [
            TorchSpecs("b"),
            TorchSpecs("b h1 h2"),
            TorchSpecs("b h", h=h+1),
            TorchSpecs("b h", dtype=torch.float),
        ]
                
        for spec in non_matching_specs:
            self.assertRaises(ValueError, lambda: spec.validate(tensor_1))
        
        # non unique dimensions
        self.assertRaises(ValueError, lambda: TorchSpecs("b b")) 
        # unknown dimensions
        self.assertRaises(ValueError, lambda: TorchSpecs("b h", b=1, h=2, c=3)) 
        self.assertRaises(ValueError, lambda: TorchSpecs("b1", b2=1))
        # zero dimensions
        self.assertRaises(ValueError, lambda: TorchSpecs("b h", b=1, h=0)) 
        # non-integer dimension
        self.assertRaises(ValueError, lambda: TorchSpecs("b h", b=1, h='h')) 
             
        

if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
import unittest
import numpy as np

from ray.rllib.models.specs.specs_np import NPSpecs
from ray.rllib.models.specs.specs_dict import ModelSpecDict


class TypeClass1:
    pass


class TypeClass2:
    pass


class TestModelSpecDict(unittest.TestCase):
    def test_basic_validation(self):

        h1, h2 = 3, 4
        spec_1 = ModelSpecDict(
            {
                "out_tensor_1": NPSpecs("b, h", h=h1),
                "out_tensor_2": NPSpecs("b, h", h=h2),
                "out_class_1": TypeClass1,
            }
        )

        # test validation.
        tensor_1 = {
            "out_tensor_1": np.random.randn(2, h1),
            "out_tensor_2": np.random.randn(2, h2),
            "out_class_1": TypeClass1(),
        }

        spec_1.validate(tensor_1)

        # test missing key in specs
        tensor_2 = {
            "out_tensor_1": np.random.randn(2, h1),
            "out_tensor_2": np.random.randn(2, h2),
        }

        self.assertRaises(ValueError, lambda: spec_1.validate(tensor_2))

        # test missing key in data
        tensor_3 = {
            "out_tensor_1": np.random.randn(2, h1),
            "out_tensor_2": np.random.randn(2, h2),
            "out_class_1": TypeClass1(),
            "out_class_2": TypeClass1(),
        }

        # this should pass because exact_match is False
        spec_1.validate(tensor_3, exact_match=False)

        # this should fail because exact_match is True
        self.assertRaises(
            ValueError, lambda: spec_1.validate(tensor_3, exact_match=True)
        )

        # raise type mismatch
        tensor_4 = {
            "out_tensor_1": np.random.randn(2, h1),
            "out_tensor_2": np.random.randn(2, h2),
            "out_class_1": TypeClass2(),
        }

        self.assertRaises(ValueError, lambda: spec_1.validate(tensor_4))

        # test nested specs
        spec_2 = ModelSpecDict(
            {
                "encoder": {
                    "input": NPSpecs("b, h", h=h1),
                    "output": NPSpecs("b, h", h=h2),
                },
                "decoder": {
                    "input": NPSpecs("b, h", h=h2),
                    "output": NPSpecs("b, h", h=h1),
                },
            }
        )

        tensor_5 = {
            "encoder": {
                "input": np.random.randn(2, h1),
                "output": np.random.randn(2, h2),
            },
            "decoder": {
                "input": np.random.randn(2, h2),
                "output": np.random.randn(2, h1),
            },
        }

        spec_2.validate(tensor_5)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))

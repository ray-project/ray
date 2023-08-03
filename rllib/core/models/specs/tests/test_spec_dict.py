import unittest
import numpy as np

from ray.rllib.core.models.specs.specs_base import TensorSpec
from ray.rllib.core.models.specs.specs_dict import SpecDict
from ray.rllib.core.models.specs.checker import (
    check_input_specs,
    convert_to_canonical_format,
)
from ray.rllib.core.models.specs.checker import SpecCheckingError


class TypeClass1:
    pass


class TypeClass2:
    pass


class TestSpecDict(unittest.TestCase):
    def test_basic_validation(self):
        """Tests basic validation of SpecDict."""

        h1, h2 = 3, 4
        spec_1 = SpecDict(
            {
                "out_tensor_1": TensorSpec("b, h", h=h1, framework="np"),
                "out_tensor_2": TensorSpec("b, h", h=h2, framework="np"),
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

        # test missing key in tensor
        tensor_2 = {
            "out_tensor_1": np.random.randn(2, h1),
            "out_tensor_2": np.random.randn(2, h2),
        }

        self.assertRaises(ValueError, lambda: spec_1.validate(tensor_2))

        # test additional key in tensor (not mentioned in spec)
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
        spec_2 = SpecDict(
            {
                "encoder": {
                    "input": TensorSpec("b, h", h=h1, framework="np"),
                    "output": TensorSpec("b, h", h=h2, framework="np"),
                },
                "decoder": {
                    "input": TensorSpec("b, h", h=h2, framework="np"),
                    "output": TensorSpec("b, h", h=h1, framework="np"),
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

    def test_key_existance_specs(self):

        # One level of keys
        spec1 = convert_to_canonical_format(["foo", "bar"])

        # # This should pass
        data1 = {"foo": 1, "bar": 2}
        spec1.validate(data1)

        # This should also pass
        data2 = {"foo": {"tar": 1}, "bar": 2}
        spec1.validate(data2)

        # This should fail
        data3 = {"foo": 1}
        self.assertRaises(ValueError, lambda: spec1.validate(data3))

        # nested specs for keys
        spec2 = convert_to_canonical_format([("foo", "bar"), "tar"])

        # This should pass
        data4 = {"foo": {"bar": 1}, "tar": 2}
        spec2.validate(data4)

        # This should fail
        data5 = {"foo": 2, "tar": 2}
        self.assertRaises(ValueError, lambda: spec2.validate(data5))

        # Another way to describe nested specs for keys
        spec3 = convert_to_canonical_format({"foo": ["bar"], "tar": None})

        # This should pass
        spec3.validate(data4)

        # This should fail
        self.assertRaises(ValueError, lambda: spec3.validate(data5))

    def test_spec_check_integration(self):
        """Tests the integration of SpecDict with the check_input_specs."""

        class Model:
            @property
            def nested_key_spec(self):
                return ["a", ("b", "c"), ("d",), ("e", "f"), ("e", "g")]

            @property
            def dict_key_spec_with_none_leaves(self):
                return {
                    "a": None,
                    "b": {
                        "c": None,
                    },
                    "d": None,
                    "e": {
                        "f": None,
                        "g": None,
                    },
                }

            @property
            def spec_with_type_and_tensor_leaves(self):
                return {"a": TypeClass1, "b": TensorSpec("b, h", h=3, framework="np")}

            @check_input_specs("nested_key_spec", only_check_on_retry=False)
            def forward_nested_key(self, input_dict):
                return input_dict

            @check_input_specs(
                "dict_key_spec_with_none_leaves", only_check_on_retry=False
            )
            def forward_dict_key_with_none_leaves(self, input_dict):
                return input_dict

            @check_input_specs(
                "spec_with_type_and_tensor_leaves", only_check_on_retry=False
            )
            def forward_spec_with_type_and_tensor_leaves(self, input_dict):
                return input_dict

        model = Model()

        # test nested key spec
        input_dict_1 = {
            "a": 1,
            "b": {
                "c": 2,
                "foo": 3,
            },
            "d": 3,
            "e": {
                "f": 4,
                "g": 5,
            },
        }

        # should run fine
        model.forward_nested_key(input_dict_1)
        model.forward_dict_key_with_none_leaves(input_dict_1)

        # test missing key
        input_dict_2 = {
            "a": 1,
            "b": {
                "c": 2,
                "foo": 3,
            },
            "d": 3,
            "e": {
                "f": 4,
            },
        }

        self.assertRaises(
            SpecCheckingError, lambda: model.forward_nested_key(input_dict_2)
        )

        self.assertRaises(
            SpecCheckingError,
            lambda: model.forward_dict_key_with_none_leaves(input_dict_2),
        )

        input_dict_3 = {
            "a": TypeClass1(),
            "b": np.array([1, 2, 3]),
        }

        # should raise shape mismatch
        self.assertRaises(
            SpecCheckingError,
            lambda: model.forward_spec_with_type_and_tensor_leaves(input_dict_3),
        )


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))

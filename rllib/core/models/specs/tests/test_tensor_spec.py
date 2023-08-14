import itertools
import unittest
import numpy as np
from ray.rllib.utils import try_import_jax, try_import_tf, try_import_torch

from ray.rllib.utils.test_utils import check
from ray.rllib.core.models.specs.specs_base import TensorSpec

_, tf, _ = try_import_tf()
torch, _ = try_import_torch()
jax, _ = try_import_jax()
jnp = jax.numpy

# This makes it so that does not convert 64-bit floats to 32-bit
jax.config.update("jax_enable_x64", True)

FRAMEWORKS_TO_TEST = {"torch", "np", "tf2", "jax"}
DOUBLE_TYPE = {
    "torch": torch.float64,
    "np": np.float64,
    "tf2": tf.float64,
    "jax": jnp.float64,
}
FLOAT_TYPE = {
    "torch": torch.float32,
    "np": np.float32,
    "tf2": tf.float32,
    "jax": jnp.float32,
}


class TestSpecs(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        pass

    def test_fill(self):

        for fw in FRAMEWORKS_TO_TEST:
            double_type = DOUBLE_TYPE[fw]

            # if un-specified dims should be 1, dtype is not important
            x = TensorSpec("b,h", framework=fw).fill(float(2.0))

            # check the shape
            self.assertEqual(x.shape, (1, 1))
            # check the value
            check(x, np.array([[2.0]]))

            x = TensorSpec("b,h", b=2, h=3, framework=fw).fill(2.0)
            self.assertEqual(x.shape, (2, 3))

            x = TensorSpec(
                "b,h1,h2,h3", h1=2, h2=3, h3=3, framework=fw, dtype=double_type
            ).fill(2)
            self.assertEqual(x.shape, (1, 2, 3, 3))
            self.assertEqual(x.dtype, double_type)

    def test_validation(self):

        b, h = 2, 3

        for fw in FRAMEWORKS_TO_TEST:
            double_type = DOUBLE_TYPE[fw]
            float_type = FLOAT_TYPE[fw]

            tensor_2d = TensorSpec(
                "b,h", b=b, h=h, framework=fw, dtype=double_type
            ).fill()

            matching_specs = [
                TensorSpec("b,h", framework=fw),
                TensorSpec("b,h", h=h, framework=fw),
                TensorSpec("b,h", h=h, b=b, framework=fw),
                TensorSpec("b,h", b=b, framework=fw, dtype=double_type),
            ]

            # check if get_shape returns a tuple of ints
            shape = matching_specs[0].get_shape(tensor_2d)
            self.assertIsInstance(shape, tuple)
            print(fw)
            print(shape)
            self.assertTrue(all(isinstance(x, int) for x in shape))

            # check matching
            for spec in matching_specs:
                spec.validate(tensor_2d)

            non_matching_specs = [
                TensorSpec("b", framework=fw),
                TensorSpec("b,h1,h2", framework=fw),
                TensorSpec("b,h", h=h + 1, framework=fw),
            ]
            if fw != "jax":
                non_matching_specs.append(
                    TensorSpec("b,h", framework=fw, dtype=float_type)
                )

            for spec in non_matching_specs:
                self.assertRaises(ValueError, lambda: spec.validate(tensor_2d))

            # non unique dimensions
            self.assertRaises(ValueError, lambda: TensorSpec("b,b", framework=fw))
            # unknown dimensions
            self.assertRaises(
                ValueError, lambda: TensorSpec("b,h", b=1, h=2, c=3, framework=fw)
            )
            self.assertRaises(ValueError, lambda: TensorSpec("b1", b2=1, framework=fw))
            # zero dimensions
            self.assertRaises(
                ValueError, lambda: TensorSpec("b,h", b=1, h=0, framework=fw)
            )
            # non-integer dimension
            self.assertRaises(
                ValueError, lambda: TensorSpec("b,h", b=1, h="h", framework=fw)
            )

    def test_equal(self):

        for fw in FRAMEWORKS_TO_TEST:
            spec_eq_1 = TensorSpec("b,h", b=2, h=3, framework=fw)
            spec_eq_2 = TensorSpec("b, h", b=2, h=3, framework=fw)
            spec_eq_3 = TensorSpec(" b,  h", b=2, h=3, framework=fw)
            spec_neq_1 = TensorSpec("b, h", h=3, b=3, framework=fw)
            spec_neq_2 = TensorSpec(
                "b, h", h=3, b=3, framework=fw, dtype=DOUBLE_TYPE[fw]
            )

            self.assertTrue(spec_eq_1 == spec_eq_2)
            self.assertTrue(spec_eq_2 == spec_eq_3)
            self.assertTrue(spec_eq_1 != spec_neq_1)
            self.assertTrue(spec_eq_1 != spec_neq_2)

    def test_type_validation(self):
        # check all combinations of spec fws with tensor fws
        for spec_fw, tensor_fw in itertools.product(
            FRAMEWORKS_TO_TEST, FRAMEWORKS_TO_TEST
        ):

            spec = TensorSpec("b, h", b=2, h=3, framework=spec_fw)
            tensor = TensorSpec("b, h", b=2, h=3, framework=tensor_fw).fill(0)

            print("spec:", type(spec), ", tensor: ", type(tensor))

            if spec_fw == tensor_fw:
                spec.validate(tensor)
            else:
                self.assertRaises(ValueError, lambda: spec.validate(tensor))

    def test_no_framework_arg(self):
        """
        Test that a TensorSpec without a framework can be created and used except
        for filling.
        """
        spec = TensorSpec("b, h", b=2, h=3)
        self.assertRaises(ValueError, lambda: spec.fill(0))

        for fw in FRAMEWORKS_TO_TEST:
            tensor = TensorSpec("b, h", b=2, h=3, framework=fw).fill(0)
            spec.validate(tensor)

    def test_validate_framework(self):
        """
        Test that a TensorSpec with a framework raises an error
        when being used with a tensor from a different framework.
        """
        for spec_fw, tensor_fw in itertools.product(
            FRAMEWORKS_TO_TEST, FRAMEWORKS_TO_TEST
        ):
            spec = TensorSpec("b, h", b=2, h=3, framework=spec_fw)
            tensor = TensorSpec("b, h", b=2, h=3, framework=tensor_fw).fill(0)
            if spec_fw == tensor_fw:
                spec.validate(tensor)
            else:
                self.assertRaises(ValueError, lambda: spec.validate(tensor))

    def test_validate_dtype(self):
        """
        Test that a TensorSpec with a dtype raises an error
        when being used with a tensor from a different dtype but works otherwise.
        """

        all_types = [DOUBLE_TYPE, FLOAT_TYPE]

        for spec_types, tensor_types in itertools.product(all_types, all_types):
            for spec_fw, tensor_fw in itertools.product(
                FRAMEWORKS_TO_TEST, FRAMEWORKS_TO_TEST
            ):

                # Pick the correct types for the frameworks
                spec_type = spec_types[spec_fw]
                tensor_type = tensor_types[tensor_fw]

                print(
                    "\nTesting.." "\nspec_fw: ",
                    spec_fw,
                    "\ntensor_fw: ",
                    tensor_fw,
                    "\nspec_type: ",
                    spec_type,
                    "\ntensor_type: ",
                    tensor_type,
                )

                spec = TensorSpec("b, h", b=2, h=3, dtype=spec_type)
                tensor = TensorSpec(
                    "b, h", b=2, h=3, framework=tensor_fw, dtype=tensor_type
                ).fill(0)

                if spec_type != tensor_type:
                    self.assertRaises(ValueError, lambda: spec.validate(tensor))
                else:
                    spec.validate(tensor)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))

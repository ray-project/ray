import itertools
import unittest

from ray.rllib.core.models.base import ENCODER_OUT, STATE_OUT
from ray.rllib.core.models.configs import CNNTransposeHeadConfig
from ray.rllib.core.models.utils import ModelChecker
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.test_utils import framework_iterator

_, tf, _ = try_import_tf()
torch, _ = try_import_torch()


class TestCNNTransposeHeads(unittest.TestCase):

    def test_cnn_transpose_heads(self):
        """Tests building DeConv heads properly and checks for correct architecture."""

        # Loop through different combinations of hyperparameters.
        inputs_dimss = [[1], [50]]
        initial_image_dims = [4, 4, 96]
        cnn_transpose_filter_specifierss = [
            [[48, [4, 4], 2], [24, [4, 4], 2], [3, [4, 4], 2]],
            [[48, [4, 4], 2], [24, [4, 4], 2], [1, [4, 4], 2]],
        ]
        cnn_transpose_activations = [None, "linear", "relu"]
        cnn_transpose_use_layernorms = [False, True]
        use_biases = [False, True]

        for permutation in itertools.product(
            inputs_dimss,
            cnn_transpose_filter_specifierss,
            cnn_transpose_activations,
            cnn_transpose_use_layernorms,
            use_biases,
        ):
            (
                inputs_dims,
                cnn_transpose_filter_specifiers,
                cnn_transpose_activation,
                cnn_transpose_use_layernorm,
                use_bias,
            ) = permutation

            # These need to match the above defined stacks.
            expected_output_dims = (
                [32, 32, 3] if cnn_transpose_filter_specifiers[-1][0] == 3
                else [32, 32, 1]
            )

            print(
                f"Testing ...\n"
                f"inputs_dims: {inputs_dims}\n"
                f"initial_image_dims: {initial_image_dims}\n"
                f"cnn_transpose_filter_specifiers: {cnn_transpose_filter_specifiers}\n"
                f"cnn_transpose_activation: {cnn_transpose_activation}\n"
                f"cnn_transpose_use_layernorm: {cnn_transpose_use_layernorm}\n"
                f"use_bias: {use_bias}\n"
            )

            config = CNNTransposeHeadConfig(
                input_dims=inputs_dims,
                initial_image_dims=initial_image_dims,
                cnn_transpose_filter_specifiers=cnn_transpose_filter_specifiers,
                cnn_transpose_activation=cnn_transpose_activation,
                cnn_transpose_use_layernorm=cnn_transpose_use_layernorm,
                output_dims=expected_output_dims,
                use_bias=use_bias,
            )

            # Use a ModelChecker to compare all added models (different frameworks)
            # with each other.
            model_checker = ModelChecker(config)

            for fw in framework_iterator(frameworks=("tf2", "torch")):
                # Add this framework version of the model to our checker.
                outputs = model_checker.add(framework=fw)
                self.assertEqual(outputs.shape, (1,) + tuple(expected_output_dims))

            # Check all added models against each other.
            model_checker.check()


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))

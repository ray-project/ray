import itertools
import unittest

from ray.rllib.core.models.configs import CNNTransposeHeadConfig
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.test_utils import ModelChecker

torch, _ = try_import_torch()


class TestCNNTransposeHeads(unittest.TestCase):
    def test_cnn_transpose_heads(self):
        """Tests building DeConv heads properly and checks for correct architecture."""

        # Loop through different combinations of hyperparameters.
        inputs_dimss = [[1], [50]]
        initial_image_dims = [4, 4, 96]
        cnn_transpose_filter_specifierss = [
            (
                # 3 layers, each doubles input image (4 -> 8 -> 16 -> 32) into a
                # final RGB.
                [[48, 4, 2], [24, 4, 2], [3, 4, 2]],
                # Expected output dims (RGB).
                [32, 32, 3],
            ),
            (
                # 3 layers, each doubles input image (4 -> 8 -> 16 -> 32) into a
                # final grayscale image.
                [[48, 4, 2], [24, 4, 2], [1, 4, 2]],
                # Expected output dims (grayscale).
                [32, 32, 1],
            ),
            (
                # 1 layer, triples input "image" (4 ->12) into a final RGB image.
                [[3, 4, 3]],
                # Expected output dims (RGB).
                [12, 12, 3],
            ),
            (
                # 1 layer, triples input "image" (4 ->12) into a final grayscale image.
                [[1, 7, 3]],
                # Expected output dims (grayscale).
                [12, 12, 1],
            ),
        ]
        cnn_transpose_activations = [None, "relu", "silu"]
        cnn_transpose_use_layernorms = [False, True]
        cnn_transpose_use_biases = [False, True]

        for permutation in itertools.product(
            inputs_dimss,
            cnn_transpose_filter_specifierss,
            cnn_transpose_activations,
            cnn_transpose_use_layernorms,
            cnn_transpose_use_biases,
        ):
            (
                inputs_dims,
                cnn_transpose_filter_specifiers,
                cnn_transpose_activation,
                cnn_transpose_use_layernorm,
                cnn_transpose_use_bias,
            ) = permutation

            # Split up into filters and resulting expected output dims.
            (
                cnn_transpose_filter_specifiers,
                expected_output_dims,
            ) = cnn_transpose_filter_specifiers

            print(
                f"Testing ...\n"
                f"inputs_dims: {inputs_dims}\n"
                f"initial_image_dims: {initial_image_dims}\n"
                f"cnn_transpose_filter_specifiers: {cnn_transpose_filter_specifiers}\n"
                f"cnn_transpose_activation: {cnn_transpose_activation}\n"
                f"cnn_transpose_use_layernorm: {cnn_transpose_use_layernorm}\n"
                f"cnn_transpose_use_bias: {cnn_transpose_use_bias}\n"
            )

            config = CNNTransposeHeadConfig(
                input_dims=inputs_dims,
                initial_image_dims=initial_image_dims,
                cnn_transpose_filter_specifiers=cnn_transpose_filter_specifiers,
                cnn_transpose_activation=cnn_transpose_activation,
                cnn_transpose_use_layernorm=cnn_transpose_use_layernorm,
                cnn_transpose_use_bias=cnn_transpose_use_bias,
            )

            # Use a ModelChecker to compare all added models (different frameworks)
            # with each other.
            model_checker = ModelChecker(config)

            # Add this framework version of the model to our checker.
            outputs = model_checker.add(framework="torch", obs=False)
            self.assertEqual(outputs.shape, (1,) + tuple(expected_output_dims))

            # Check all added models against each other.
            model_checker.check()


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))

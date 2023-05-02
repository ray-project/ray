import itertools
import unittest

from ray.rllib.core.models.base import ENCODER_OUT
from ray.rllib.core.models.configs import CNNEncoderConfig
from ray.rllib.models.utils import get_filter_config
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.test_utils import framework_iterator, ModelChecker

_, tf, _ = try_import_tf()
torch, _ = try_import_torch()


class TestCNNEncoders(unittest.TestCase):
    def test_cnn_encoders(self):
        """Tests building CNN encoders properly and checks for correct architecture."""

        # Loop through different combinations of hyperparameters.
        inputs_dimss = [
            [480, 640, 3],
            [480, 640, 1],
            [240, 320, 3],
            [240, 320, 1],
            [96, 96, 3],
            [96, 96, 1],
            [84, 84, 3],
            [84, 84, 1],
            [64, 64, 3],
            [64, 64, 1],
            [42, 42, 3],
            [42, 42, 1],
            [10, 10, 3],
        ]
        cnn_activations = [None, "linear", "relu"]
        cnn_use_layernorms = [False, True]
        output_dimss = [[1], [100]]
        output_activations = cnn_activations
        use_biases = [False, True]

        for permutation in itertools.product(
            inputs_dimss,
            cnn_activations,
            cnn_use_layernorms,
            output_activations,
            output_dimss,
            use_biases,
        ):
            (
                inputs_dims,
                cnn_activation,
                cnn_use_layernorm,
                output_activation,
                output_dims,
                use_bias,
            ) = permutation

            filter_specifiers = get_filter_config(inputs_dims)

            print(
                f"Testing ...\n"
                f"input_dims: {inputs_dims}\n"
                f"cnn_filter_specifiers: {filter_specifiers}\n"
                f"cnn_activation: {cnn_activation}\n"
                f"cnn_use_layernorm: {cnn_use_layernorm}\n"
                f"output_activation: {output_activation}\n"
                f"output_dims: {output_dims}\n"
                f"use_bias: {use_bias}\n"
            )

            config = CNNEncoderConfig(
                input_dims=inputs_dims,
                cnn_filter_specifiers=filter_specifiers,
                cnn_activation=cnn_activation,
                cnn_use_layernorm=cnn_use_layernorm,
                output_dims=output_dims,
                output_activation=output_activation,
                use_bias=use_bias,
            )

            # Use a ModelChecker to compare all added models (different frameworks)
            # with each other.
            model_checker = ModelChecker(config)

            for fw in framework_iterator(frameworks=("tf2", "torch")):
                # Add this framework version of the model to our checker.
                outputs = model_checker.add(framework=fw)
                self.assertEqual(outputs[ENCODER_OUT].shape, (1, output_dims[0]))

            # Check all added models against each other.
            model_checker.check()


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))

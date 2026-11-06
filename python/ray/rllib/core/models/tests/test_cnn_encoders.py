import itertools
import unittest

from ray.rllib.core.models.base import ENCODER_OUT
from ray.rllib.core.models.configs import CNNEncoderConfig
from ray.rllib.models.utils import get_filter_config
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.test_utils import ModelChecker

torch, _ = try_import_torch()


class TestCNNEncoders(unittest.TestCase):
    def test_cnn_encoders(self):
        """Tests building CNN encoders properly and checks for correct architecture."""

        # Loop through permutations of hyperparameters.
        inputs_dimss = [
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
        cnn_use_biases = [False, True]

        for permutation in itertools.product(
            inputs_dimss,
            cnn_activations,
            cnn_use_layernorms,
            cnn_use_biases,
        ):
            (
                inputs_dims,
                cnn_activation,
                cnn_use_layernorm,
                cnn_use_bias,
            ) = permutation

            filter_specifiers = get_filter_config(inputs_dims)

            print(
                f"Testing ...\n"
                f"input_dims: {inputs_dims}\n"
                f"cnn_filter_specifiers: {filter_specifiers}\n"
                f"cnn_activation: {cnn_activation}\n"
                f"cnn_use_layernorm: {cnn_use_layernorm}\n"
                f"cnn_use_bias: {cnn_use_bias}\n"
            )

            config = CNNEncoderConfig(
                input_dims=inputs_dims,
                cnn_filter_specifiers=filter_specifiers,
                cnn_activation=cnn_activation,
                cnn_use_layernorm=cnn_use_layernorm,
                cnn_use_bias=cnn_use_bias,
            )

            # Use a ModelChecker to compare all added models (different frameworks)
            # with each other.
            model_checker = ModelChecker(config)

            # Add this framework version of the model to our checker.
            outputs = model_checker.add(framework="torch")
            # Confirm that the config conputed the correct (actual) output dims.
            self.assertEqual(outputs[ENCODER_OUT].shape, (1, config.output_dims[0]))

            # Check all added models against each other.
            model_checker.check()

    def test_cnn_encoders_valid_padding(self):
        """Tests building CNN encoders with valid padding."""

        inputs_dims = [42, 42, 3]

        # Test filter specifier with "valid"-padding setting in it.
        # Historical fun fact: The following was our old default CNN setup for
        # (42, 42, 3) Atari image spaces. The last layer, with its hard-coded :( padding
        # setting was narrowing down the image size to 1x1, so we could use the last
        # layer already as a 256-sized pre-logits layer (normally done by a Dense).
        filter_specifiers = [[16, 4, 2, "same"], [32, 4, 2], [256, 11, 1, "valid"]]

        config = CNNEncoderConfig(
            input_dims=inputs_dims,
            cnn_filter_specifiers=filter_specifiers,
        )

        # Use a ModelChecker to compare all added models (different frameworks)
        # with each other.
        model_checker = ModelChecker(config)

        # Add this framework version of the model to our checker.
        outputs = model_checker.add(framework="torch")
        # Confirm that the config conputed the correct (actual) output dims.
        self.assertEqual(outputs[ENCODER_OUT].shape, (1, config.output_dims[0]))

        # Check all added models against each other.
        model_checker.check()


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))

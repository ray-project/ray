import itertools
import unittest

from ray.rllib.core.models.base import ENCODER_OUT
from ray.rllib.core.models.configs import MLPEncoderConfig
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.test_utils import ModelChecker

torch, _ = try_import_torch()


class TestMLPEncoders(unittest.TestCase):
    def test_mlp_encoders(self):
        """Tests building MLP encoders properly and checks for correct architecture."""

        # Loop through different combinations of hyperparameters.
        inputs_dims_configs = [[1], [50]]
        list_of_hidden_layer_dims = [[], [1], [64, 64], [256, 256, 256]]
        hidden_layer_activations = [None, "linear", "relu", "tanh", "swish"]
        hidden_layer_use_layernorms = [False, True]
        output_dims = [1, 48]
        output_activations = hidden_layer_activations
        hidden_use_biases = [False, True]
        output_use_biases = [False, True]

        for permutation in itertools.product(
            inputs_dims_configs,
            list_of_hidden_layer_dims,
            hidden_layer_activations,
            hidden_layer_use_layernorms,
            output_activations,
            output_dims,
            hidden_use_biases,
            output_use_biases,
        ):
            (
                inputs_dims,
                hidden_layer_dims,
                hidden_layer_activation,
                hidden_layer_use_layernorm,
                output_activation,
                output_dim,
                hidden_use_bias,
                output_use_bias,
            ) = permutation

            print(
                f"Testing ...\n"
                f"input_dims: {inputs_dims}\n"
                f"hidden_layer_dims: {hidden_layer_dims}\n"
                f"hidden_layer_activation: {hidden_layer_activation}\n"
                f"hidden_layer_use_layernorm: {hidden_layer_use_layernorm}\n"
                f"output_activation: {output_activation}\n"
                f"output_dim: {output_dim}\n"
                f"hidden_use_bias: {hidden_use_bias}\n"
                f"output_use_bias: {output_use_bias}\n"
            )

            config = MLPEncoderConfig(
                input_dims=inputs_dims,
                hidden_layer_dims=hidden_layer_dims,
                hidden_layer_activation=hidden_layer_activation,
                hidden_layer_use_layernorm=hidden_layer_use_layernorm,
                hidden_layer_use_bias=hidden_use_bias,
                output_layer_dim=output_dim,
                output_layer_activation=output_activation,
                output_layer_use_bias=output_use_bias,
            )

            # Use a ModelChecker to compare all added models (different frameworks)
            # with each other.
            model_checker = ModelChecker(config)

            # Add this framework version of the model to our checker.
            outputs = model_checker.add(framework="torch")
            self.assertEqual(outputs[ENCODER_OUT].shape, (1, output_dim))

            # Check all added models against each other.
            model_checker.check()


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))

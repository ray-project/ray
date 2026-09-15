import itertools
import unittest

from ray.rllib.core.models.configs import FreeLogStdMLPHeadConfig, MLPHeadConfig
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.test_utils import ModelChecker

torch, nn = try_import_torch()


class TestMLPHeads(unittest.TestCase):
    def test_mlp_heads(self):
        """Tests building MLP heads properly and checks for correct architecture."""

        # Loop through different combinations of hyperparameters.
        inputs_dims_configs = [[1], [50]]
        list_of_hidden_layer_dims = [[], [1], [64, 64], [512, 512]]
        hidden_layer_activations = ["linear", "relu", "swish"]
        hidden_layer_use_layernorms = [False, True]
        # Can only test even `output_dims` for FreeLogStdMLPHeadConfig.
        output_dims = [2, 50]
        output_activations = hidden_layer_activations
        hidden_use_biases = [False, True]
        output_use_biases = [False, True]
        free_stds = [False, True]

        for permutation in itertools.product(
            inputs_dims_configs,
            list_of_hidden_layer_dims,
            hidden_layer_activations,
            hidden_layer_use_layernorms,
            output_activations,
            output_dims,
            hidden_use_biases,
            output_use_biases,
            free_stds,
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
                free_std,
            ) = permutation

            print(
                f"Testing ...\n"
                f"input_dims: {inputs_dims}\n"
                f"hidden_layer_dims: {hidden_layer_dims}\n"
                f"hidden_layer_activation: {hidden_layer_activation}\n"
                f"hidden_layer_use_layernorm: {hidden_layer_use_layernorm}\n"
                f"output_activation: {output_activation}\n"
                f"output_dim: {output_dim}\n"
                f"free_std: {free_std}\n"
                f"hidden_use_bias: {hidden_use_bias}\n"
                f"output_use_bias: {output_use_bias}\n"
            )

            config_cls = FreeLogStdMLPHeadConfig if free_std else MLPHeadConfig
            config = config_cls(
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
            outputs = model_checker.add(framework="torch", obs=False)
            self.assertEqual(outputs.shape, (1, output_dim))

            # Check all added models against each other.
            model_checker.check()


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))

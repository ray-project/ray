import itertools
import unittest

from ray.rllib.core.models.configs import MLPHeadConfig, FreeLogStdMLPHeadConfig
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.test_utils import framework_iterator, ModelChecker

_, tf, _ = try_import_tf()
torch, nn = try_import_torch()


class TestMLPHeads(unittest.TestCase):
    def test_mlp_heads(self):
        """Tests building MLP heads properly and checks for correct architecture."""

        # Loop through different combinations of hyperparameters.
        inputs_dims_configs = [[1], [50]]
        list_of_hidden_layer_dims = [[], [1], [64, 64], [512, 512]]
        hidden_layer_activations = ["linear", "relu", "tanh", "swish"]
        hidden_layer_use_layernorms = [False, True]
        # Can only test even `output_dims` for FreeLogStdMLPHeadConfig.
        output_dims_configs = [[2], [50]]
        output_activations = hidden_layer_activations
        use_biases = [False, True]
        free_stds = [False, True]

        for permutation in itertools.product(
            inputs_dims_configs,
            list_of_hidden_layer_dims,
            hidden_layer_activations,
            hidden_layer_use_layernorms,
            output_activations,
            output_dims_configs,
            use_biases,
            free_stds,
        ):
            (
                inputs_dims,
                hidden_layer_dims,
                hidden_layer_activation,
                hidden_layer_use_layernorm,
                output_activation,
                output_dims,
                use_bias,
                free_std,
            ) = permutation

            print(
                f"Testing ...\n"
                f"input_dims: {inputs_dims}\n"
                f"hidden_layer_dims: {hidden_layer_dims}\n"
                f"hidden_layer_activation: {hidden_layer_activation}\n"
                f"hidden_layer_use_layernorm: {hidden_layer_use_layernorm}\n"
                f"output_activation: {output_activation}\n"
                f"output_dims: {output_dims}\n"
                f"free_std: {free_std}\n"
                f"use_bias: {use_bias}\n"
            )

            config_cls = FreeLogStdMLPHeadConfig if free_std else MLPHeadConfig
            config = config_cls(
                input_dims=inputs_dims,
                hidden_layer_dims=hidden_layer_dims,
                hidden_layer_activation=hidden_layer_activation,
                hidden_layer_use_layernorm=hidden_layer_use_layernorm,
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
                self.assertEqual(outputs.shape, (1, output_dims[0]))

            # Check all added models against each other.
            model_checker.check()


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))

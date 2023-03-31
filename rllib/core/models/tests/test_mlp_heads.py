import itertools
import unittest

from ray.rllib.core.models.configs import MLPHeadConfig, FreeLogStdMLPHeadConfig
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.test_utils import framework_iterator

_, tf, _ = try_import_tf()
torch, nn = try_import_torch()


class TestMLPHeads(unittest.TestCase):

    def test_mlp_heads(self):
        """Tests building MLP heads properly and checks for correct architecture."""

        inputs_dims_configs = [[1], [50]]

        list_of_hidden_layer_dims = [[], [1], [64, 64], [1000, 1000, 1000]]

        hidden_layer_activations = [None, "linear", "relu", "tanh", "swish"]

        hidden_layer_use_layernorms = [False, True]

        # Can only test even output_dims for FreeLogStdMLPHeadConfig.
        output_dims_configs = [[2], [1000]]

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
                f"inputs_dim: {inputs_dims}\n"
                f"hidden_layer_dims: {hidden_layer_dims}\n"
                f"hidden_layer_activation: {hidden_layer_activation}\n"
                f"hidden_layer_use_layernorm: {hidden_layer_use_layernorm}\n"
                f"output_activation: {output_activation}\n"
                f"output_dims: {output_dims}\n"
                f"free_std: {free_std}\n"
                f"use_bias: {use_bias}\n"
            )

            config = MLPHeadConfig(
                input_dims=inputs_dims,
                hidden_layer_dims=hidden_layer_dims,
                hidden_layer_activation=hidden_layer_activation,
                hidden_layer_use_layernorm=hidden_layer_use_layernorm,
                output_dims=output_dims,
                output_activation=output_activation,
                use_bias=use_bias,
            )

            if free_std:
                config = FreeLogStdMLPHeadConfig(mlp_head_config=config)

            # To compare number of params between frameworks.
            tf_counts = None

            for fw in framework_iterator(frameworks=("tf2", "torch")):

                model = config.build(framework=fw)
                print(model)

                # Pass a B=1 observation through the model.
                if fw == "tf2":
                    inputs = tf.random.uniform([1] + inputs_dims)
                else:
                    inputs = torch.randn(1, inputs_dims[0])

                outputs = model(inputs)

                if fw == "tf2":
                    tf_counts = model.get_num_parameters()
                else:
                    torch_counts = model.get_num_parameters()
                    # Compare number of trainable and non-trainable params between
                    # tf and torch.
                    self.assertEqual(torch_counts[0], tf_counts[0])
                    self.assertEqual(torch_counts[1], tf_counts[1])

                self.assertEqual(outputs.shape, (1, output_dims[0]))


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))

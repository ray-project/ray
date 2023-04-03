import itertools
import unittest

import numpy as np

from ray.rllib.core.models.configs import MLPHeadConfig, FreeLogStdMLPHeadConfig
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.test_utils import framework_iterator

_, tf, _ = try_import_tf()
torch, nn = try_import_torch()


class TestMLPHeads(unittest.TestCase):
    def test_mlp_heads(self):
        """Tests building MLP heads properly and checks for correct architecture."""

        # Loop through different combinations of hyperparameters.
        inputs_dims_configs = [[1], [50]]
        list_of_hidden_layer_dims = [[], [1], [64, 64], [1000, 1000, 1000]]
        hidden_layer_activations = [None, "linear", "relu", "tanh", "swish"]
        hidden_layer_use_layernorms = [False, True]
        # Can only test even `output_dims` for FreeLogStdMLPHeadConfig.
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

            # To compare number of params between frameworks.
            param_counts = {}
            # To compare computed outputs from fixed-weights-nets between frameworks.
            output_values = {}

            # We will pass an observation filled with this one random value through
            # all DL networks (after they have been set to fixed-weights) to compare
            # the computed outputs.
            random_fill_input_value = np.random.uniform(-0.1, 0.1)

            for fw in framework_iterator(frameworks=("tf2", "torch")):

                model = config.build(framework=fw)
                print(model)

                # Pass a B=1 observation through the model.
                if fw == "tf2":
                    inputs = tf.fill([1] + [inputs_dims[0]], random_fill_input_value)
                else:
                    inputs = torch.full([1] + [inputs_dims[0]], random_fill_input_value)

                outputs = model(inputs)
                # Bring model into a reproducible, comparable state (so we can compare
                # computations across frameworks).
                model._set_to_dummy_weights(value_sequence=(random_fill_input_value,))
                # And do another forward pass.
                comparable_outputs = model(inputs)

                # Store the number of parameters for this framework's net.
                param_counts[fw] = model.get_num_parameters()
                # Store the fixed-weights-net outputs for this framework's net.
                if fw == "tf2":
                    output_values[fw] = comparable_outputs.numpy()
                else:
                    output_values[fw] = comparable_outputs.detach().numpy()

                self.assertEqual(outputs.shape, (1, output_dims[0]))

            # Compare number of trainable and non-trainable params between all
            # frameworks.
            self.assertTrue(
                np.all([c == param_counts["tf2"] for c in param_counts.values()])
            )
            # Compare dummy outputs by exact values given that all nets received the
            # same input and all nets have the same (dummy) weight values.
            self.assertTrue(np.all([
                np.allclose(v, output_values["tf2"], rtol=0.001)
                for v in output_values.values()
            ]))


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))

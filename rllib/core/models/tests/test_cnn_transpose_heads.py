import itertools
import unittest

from ray.rllib.core.models.base import ENCODER_OUT, STATE_OUT
from ray.rllib.core.models.configs import CNNTransposeHeadConfig
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.test_utils import framework_iterator

_, tf, _ = try_import_tf()
torch, _ = try_import_torch()


class TestCNNTransposeHeads(unittest.TestCase):

    def test_cnn_transpose_heads(self):
        """Tests building DeConv heads properly and checks for correct architecture."""

        inputs_dimss = [[1], [50]]

        initial_dense_layer_output_dims = [4, 4, 96]

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
                f"initial_dense_layer_output_dims: {initial_dense_layer_output_dims}\n"
                f"cnn_transpose_filter_specifiers: {cnn_transpose_filter_specifiers}\n"
                f"cnn_transpose_activation: {cnn_transpose_activation}\n"
                f"cnn_transpose_use_layernorm: {cnn_transpose_use_layernorm}\n"
                f"use_bias: {use_bias}\n"
            )

            config = CNNTransposeHeadConfig(
                input_dims=inputs_dims,
                initial_dense_layer_output_dims=initial_dense_layer_output_dims,
                cnn_transpose_filter_specifiers=cnn_transpose_filter_specifiers,
                cnn_transpose_activation=cnn_transpose_activation,
                cnn_transpose_use_layernorm=cnn_transpose_use_layernorm,
                output_dims=expected_output_dims,
                use_bias=use_bias,
            )

            # To compare number of params between frameworks.
            tf_counts = None

            for fw in framework_iterator(frameworks=("tf2", "torch")):
                model = config.build(framework=fw)
                print(model)

                # Pass a B=1 observation through the model.
                if fw == "tf2":
                    obs = tf.random.uniform([1] + inputs_dims)
                else:
                    obs = torch.randn(1, *inputs_dims)

                outputs = model(obs)

                if fw == "tf2":
                    tf_counts = model.get_num_parameters()
                else:
                    torch_counts = model.get_num_parameters()
                    # Compare number of trainable and non-trainable params between
                    # tf and torch.
                    self.assertEqual(torch_counts[0], tf_counts[0])
                    self.assertEqual(torch_counts[1], tf_counts[1])

                self.assertEqual(outputs.shape, (1,) + tuple(expected_output_dims))


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))

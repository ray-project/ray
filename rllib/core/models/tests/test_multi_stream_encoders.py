import itertools
import unittest

from ray.rllib.core.models.base import ENCODER_OUT
from ray.rllib.core.models.configs import MLPEncoderConfig, MultiStreamEncoderConfig
from ray.rllib.utils.framework import try_import_torch

torch, _ = try_import_torch()


class TestMultiStreamEncoders(unittest.TestCase):
    def test_multi_stream_encoders(self):
        """Tests building MultiStream encoders and checks for correct architecture."""

        # Loop through different combinations of hyperparameters.
        base_encoder_configs_list = [
            # Two streams with same config
            {
                "stream_a": MLPEncoderConfig(
                    input_dims=[8],
                    hidden_layer_dims=[32],
                    output_layer_dim=16,
                ),
                "stream_b": MLPEncoderConfig(
                    input_dims=[4],
                    hidden_layer_dims=[16],
                    output_layer_dim=16,
                ),
            },
            # Three streams with different configs
            {
                "stream_a": MLPEncoderConfig(
                    input_dims=[10],
                    hidden_layer_dims=[64, 64],
                    output_layer_dim=32,
                ),
                "stream_b": MLPEncoderConfig(
                    input_dims=[5],
                    hidden_layer_dims=[32],
                    output_layer_dim=16,
                ),
                "stream_c": MLPEncoderConfig(
                    input_dims=[3],
                    hidden_layer_dims=[16],
                    output_layer_dim=8,
                ),
            },
        ]
        hidden_layer_dims_list = [[64], [128, 128], [256, 256, 256]]
        hidden_layer_activations = ["relu", "tanh"]
        hidden_layer_use_biases = [False, True]
        output_layer_dims = [32, 64]
        output_layer_activations = ["linear", "relu"]
        output_layer_use_biases = [False, True]

        for permutation in itertools.product(
            base_encoder_configs_list,
            hidden_layer_dims_list,
            hidden_layer_activations,
            hidden_layer_use_biases,
            output_layer_dims,
            output_layer_activations,
            output_layer_use_biases,
        ):
            (
                base_encoder_configs,
                hidden_layer_dims,
                hidden_layer_activation,
                hidden_layer_use_bias,
                output_layer_dim,
                output_layer_activation,
                output_layer_use_bias,
            ) = permutation

            print(
                f"Testing ...\n"
                f"num_streams: {len(base_encoder_configs)}\n"
                f"hidden_layer_dims: {hidden_layer_dims}\n"
                f"hidden_layer_activation: {hidden_layer_activation}\n"
                f"hidden_layer_use_bias: {hidden_layer_use_bias}\n"
                f"output_layer_dim: {output_layer_dim}\n"
                f"output_layer_activation: {output_layer_activation}\n"
                f"output_layer_use_bias: {output_layer_use_bias}\n"
            )

            config = MultiStreamEncoderConfig(
                base_encoder_configs=base_encoder_configs,
                hidden_layer_dims=hidden_layer_dims,
                hidden_layer_activation=hidden_layer_activation,
                hidden_layer_use_bias=hidden_layer_use_bias,
                output_layer_dim=output_layer_dim,
                output_layer_activation=output_layer_activation,
                output_layer_use_bias=output_layer_use_bias,
            )

            # Build the model.
            model = config.build(framework="torch")

            # Create dummy inputs for each stream.
            batch_size = 4
            inputs = {
                k: torch.randn(batch_size, cfg.input_dims[0])
                for k, cfg in base_encoder_configs.items()
            }

            # Forward pass.
            outputs = model(inputs)

            # Check output shape.
            self.assertEqual(outputs[ENCODER_OUT].shape, (batch_size, output_layer_dim))

    def test_multi_stream_encoder_weight_initializers(self):
        """Tests custom weight and bias initializers for MultiStreamEncoder."""

        base_encoder_configs = {
            "obs": MLPEncoderConfig(
                input_dims=[8],
                hidden_layer_dims=[32],
                output_layer_dim=16,
            ),
            "actions": MLPEncoderConfig(
                input_dims=[4],
                hidden_layer_dims=[16],
                output_layer_dim=8,
            ),
        }

        config = MultiStreamEncoderConfig(
            base_encoder_configs=base_encoder_configs,
            hidden_layer_dims=[64, 64],
            hidden_layer_activation="relu",
            hidden_layer_weights_initializer="xavier_uniform_",
            hidden_layer_bias_initializer="zeros_",
            output_layer_dim=32,
            output_layer_activation="linear",
            output_layer_weights_initializer="xavier_uniform_",
            output_layer_bias_initializer="zeros_",
        )

        model = config.build(framework="torch")

        # Check that biases are initialized to zeros.
        for layer in model.fusion_layers:
            if hasattr(layer, "bias") and layer.bias is not None:
                self.assertTrue(
                    torch.allclose(layer.bias, torch.zeros_like(layer.bias))
                )

    def test_multi_stream_encoder_sorted_keys(self):
        """Tests that stream processing order is deterministic (sorted keys)."""

        # Create configs with unsorted keys.
        base_encoder_configs = {
            "z_stream": MLPEncoderConfig(
                input_dims=[4],
                hidden_layer_dims=[16],
                output_layer_dim=8,
            ),
            "a_stream": MLPEncoderConfig(
                input_dims=[8],
                hidden_layer_dims=[32],
                output_layer_dim=16,
            ),
        }

        config = MultiStreamEncoderConfig(
            base_encoder_configs=base_encoder_configs,
            hidden_layer_dims=[64],
            output_layer_dim=32,
        )

        model = config.build(framework="torch")

        # Verify base_encoders are in sorted order.
        encoder_keys = list(model.base_encoders.keys())
        self.assertEqual(encoder_keys, sorted(encoder_keys))

    def test_multi_stream_encoder_optional_output_layer(self):
        """Tests MultiStreamEncoder with and without an output layer."""

        base_encoder_configs = {
            "obs": MLPEncoderConfig(
                input_dims=[8],
                hidden_layer_dims=[32],
                output_layer_dim=16,
            ),
            "actions": MLPEncoderConfig(
                input_dims=[4],
                hidden_layer_dims=[16],
                output_layer_dim=8,
            ),
        }
        # Total embed dim = 16 + 8 = 24

        batch_size = 4

        # Test WITH output layer.
        config_with_output = MultiStreamEncoderConfig(
            base_encoder_configs=base_encoder_configs,
            hidden_layer_dims=[64, 64],
            output_layer_dim=32,
        )
        model_with_output = config_with_output.build(framework="torch")

        # Verify output layer exists.
        self.assertIsNotNone(model_with_output.output_layer)
        self.assertEqual(model_with_output.output_layer.out_features, 32)

        inputs = {
            "obs": torch.randn(batch_size, 8),
            "actions": torch.randn(batch_size, 4),
        }
        outputs = model_with_output(inputs)
        self.assertEqual(outputs[ENCODER_OUT].shape, (batch_size, 32))

        # Test WITHOUT output layer.
        config_without_output = MultiStreamEncoderConfig(
            base_encoder_configs=base_encoder_configs,
            hidden_layer_dims=[64, 64],
            output_layer_dim=None,
        )
        model_without_output = config_without_output.build(framework="torch")

        # Verify output layer does not exist.
        self.assertIsNone(model_without_output.output_layer)

        outputs = model_without_output(inputs)
        # Output should be last hidden layer dim (64).
        self.assertEqual(outputs[ENCODER_OUT].shape, (batch_size, 64))

        # Test WITHOUT output layer AND without hidden layers.
        config_no_layers = MultiStreamEncoderConfig(
            base_encoder_configs=base_encoder_configs,
            hidden_layer_dims=[],
            output_layer_dim=None,
        )
        model_no_layers = config_no_layers.build(framework="torch")

        self.assertIsNone(model_no_layers.output_layer)
        self.assertEqual(len(model_no_layers.fusion_layers), 0)

        outputs = model_no_layers(inputs)
        # Output should be concatenated base encoder outputs (16 + 8 = 24).
        self.assertEqual(outputs[ENCODER_OUT].shape, (batch_size, 24))

        # Test WITH output layer but without hidden layers.
        config_output_only = MultiStreamEncoderConfig(
            base_encoder_configs=base_encoder_configs,
            hidden_layer_dims=[],
            output_layer_dim=48,
        )
        model_output_only = config_output_only.build(framework="torch")

        self.assertIsNotNone(model_output_only.output_layer)
        self.assertEqual(len(model_output_only.fusion_layers), 0)
        # Input to output layer should be total_embed_dim (24).
        self.assertEqual(model_output_only.output_layer.in_features, 24)

        outputs = model_output_only(inputs)
        self.assertEqual(outputs[ENCODER_OUT].shape, (batch_size, 48))


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))

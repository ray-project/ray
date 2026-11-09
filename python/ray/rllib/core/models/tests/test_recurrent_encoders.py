import itertools
import unittest

import numpy as np
import pytest

from ray.rllib.core.columns import Columns
from ray.rllib.core.models.base import ENCODER_OUT
from ray.rllib.core.models.configs import RecurrentEncoderConfig
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.test_utils import ModelChecker

torch, _ = try_import_torch()


class TestRecurrentEncoders(unittest.TestCase):
    def test_gru_encoders(self):
        """Tests building GRU encoders properly and checks for correct architecture."""

        # Loop through different combinations of hyperparameters.
        inputs_dimss = [[1], [100]]
        num_layerss = [1, 4]
        hidden_dims = [128, 256]
        use_biases = [False, True]

        for permutation in itertools.product(
            inputs_dimss,
            num_layerss,
            hidden_dims,
            use_biases,
        ):
            (
                inputs_dims,
                num_layers,
                hidden_dim,
                use_bias,
            ) = permutation

            print(
                f"Testing ...\n"
                f"input_dims: {inputs_dims}\n"
                f"num_layers: {num_layers}\n"
                f"hidden_dim: {hidden_dim}\n"
                f"use_bias: {use_bias}\n"
            )

            config = RecurrentEncoderConfig(
                recurrent_layer_type="gru",
                input_dims=inputs_dims,
                num_layers=num_layers,
                hidden_dim=hidden_dim,
                use_bias=use_bias,
            )

            # Use a ModelChecker to compare all added models (different frameworks)
            # with each other.
            model_checker = ModelChecker(config)

            # Add this framework version of the model to our checker.
            outputs = model_checker.add(
                framework="torch", state={"h": np.array([num_layers, hidden_dim])}
            )
            # Output shape: [1=B, 1=T, [output_dim]]
            self.assertEqual(
                outputs[ENCODER_OUT].shape,
                (1, 1, config.output_dims[0]),
            )
            # State shapes: [1=B, 1=num_layers, [hidden_dim]]
            self.assertEqual(
                outputs[Columns.STATE_OUT]["h"].shape,
                (1, num_layers, hidden_dim),
            )
            # Check all added models against each other.
            model_checker.check()

    def test_lstm_encoders(self):
        """Tests building LSTM encoders properly and checks for correct architecture."""

        # Loop through different combinations of hyperparameters.
        inputs_dimss = [[1], [100]]
        num_layerss = [1, 3]
        hidden_dims = [16, 128]
        use_biases = [False, True]

        for permutation in itertools.product(
            inputs_dimss,
            num_layerss,
            hidden_dims,
            use_biases,
        ):
            (
                inputs_dims,
                num_layers,
                hidden_dim,
                use_bias,
            ) = permutation

            print(
                f"Testing ...\n"
                f"input_dims: {inputs_dims}\n"
                f"num_layers: {num_layers}\n"
                f"hidden_dim: {hidden_dim}\n"
                f"use_bias: {use_bias}\n"
            )

            config = RecurrentEncoderConfig(
                recurrent_layer_type="lstm",
                input_dims=inputs_dims,
                num_layers=num_layers,
                hidden_dim=hidden_dim,
                use_bias=use_bias,
            )

            # Use a ModelChecker to compare all added models (different frameworks)
            # with each other.
            model_checker = ModelChecker(config)

            # Add this framework version of the model to our checker.
            outputs = model_checker.add(
                framework="torch",
                state={
                    "h": np.array([num_layers, hidden_dim]),
                    "c": np.array([num_layers, hidden_dim]),
                },
            )
            # Output shape: [1=B, 1=T, [output_dim]]
            self.assertEqual(
                outputs[ENCODER_OUT].shape,
                (1, 1, config.output_dims[0]),
            )
            # State shapes: [1=B, 1=num_layers, [hidden_dim]]
            self.assertEqual(
                outputs[Columns.STATE_OUT]["h"].shape,
                (1, num_layers, hidden_dim),
            )
            self.assertEqual(
                outputs[Columns.STATE_OUT]["c"].shape,
                (1, num_layers, hidden_dim),
            )

            # Check all added models against each other (only if bias=False).
            # See here on why pytorch uses two bias vectors per layer and tf only uses
            # one:
            # https://towardsdatascience.com/implementation-differences-in-lstm-
            # layers-tensorflow-vs-pytorch-77a31d742f74
            if use_bias is False:
                model_checker.check()

    def test_lstm_encoder_onnx_export_with_dynamic_batch(self):
        """Tests exporting an LSTM encoder to ONNX with a dynamic batch dim."""
        pytest.importorskip("onnxruntime")
        pytest.importorskip("onnxscript")
        encoder = RecurrentEncoderConfig(
            recurrent_layer_type="lstm", input_dims=[4], num_layers=2, hidden_dim=16
        ).build(framework="torch")

        class FlatEncoder(torch.nn.Module):
            # The exporter takes flat tensors, not the encoder's nested dicts.
            def __init__(self):
                super().__init__()
                self.encoder = encoder

            def forward(self, obs, h, c):
                out = self.encoder(
                    {Columns.OBS: obs, Columns.STATE_IN: {"h": h, "c": c}}
                )
                state_out = out[Columns.STATE_OUT]
                return out[ENCODER_OUT], state_out["h"], state_out["c"]

        def inputs(batch_size):
            return tuple(
                torch.randn(batch_size, *shape) for shape in [(1, 4), (2, 16), (2, 16)]
            )

        model = FlatEncoder().eval()
        # Export from a batch of two: from a batch of one, torch may fix the batch
        # dim at one.
        batch = torch.export.Dim("batch")
        onnx_program = torch.onnx.export(
            model, inputs(2), dynamic_shapes=[{0: batch}] * 3, dynamo=True
        )
        x = inputs(3)
        with torch.no_grad():
            # Calling the exported program runs it in onnxruntime.
            torch.testing.assert_close(
                list(onnx_program(*x)), list(model(*x)), rtol=1e-5, atol=1e-5
            )


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))

import itertools
import os
import tempfile
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
        """Tests exporting an LSTM encoder to ONNX with a dynamic batch dim.

        Exports with the dynamo-based exporter from one step of two sequences -- from
        a batch of one, torch may fix the batch dim at one -- and runs the exported
        model on one and on three sequences.
        """
        onnxruntime = pytest.importorskip("onnxruntime")
        # The dynamo-based exporter translates the graph with onnxscript.
        pytest.importorskip("onnxscript")

        class FlatLSTMEncoder(torch.nn.Module):
            # The exporter traces flat tensors in and out, not the encoder's nested
            # input and output dicts.
            def __init__(self, encoder):
                super().__init__()
                self.encoder = encoder

            def forward(self, obs, state_in_h, state_in_c):
                outputs = self.encoder(
                    {
                        Columns.OBS: obs,
                        Columns.STATE_IN: {"h": state_in_h, "c": state_in_c},
                    }
                )
                state_out = outputs[Columns.STATE_OUT]
                return outputs[ENCODER_OUT], state_out["h"], state_out["c"]

        input_names = ["obs", "state_in_h", "state_in_c"]
        input_dim, hidden_dim = 4, 16
        for num_layers in [1, 2]:
            with self.subTest(num_layers=num_layers):
                config = RecurrentEncoderConfig(
                    recurrent_layer_type="lstm",
                    input_dims=[input_dim],
                    num_layers=num_layers,
                    hidden_dim=hidden_dim,
                )
                model = FlatLSTMEncoder(config.build(framework="torch")).eval()

                def make_inputs(batch_size):
                    rng = np.random.default_rng(batch_size)
                    return [
                        torch.from_numpy(rng.standard_normal(shape, dtype=np.float32))
                        for shape in [
                            (batch_size, 1, input_dim),
                            (batch_size, num_layers, hidden_dim),
                            (batch_size, num_layers, hidden_dim),
                        ]
                    ]

                batch = torch.export.Dim("batch")
                with tempfile.TemporaryDirectory() as tmpdir:
                    path = os.path.join(tmpdir, "lstm_encoder.onnx")
                    torch.onnx.export(
                        model,
                        tuple(make_inputs(2)),
                        f=path,
                        input_names=input_names,
                        dynamic_shapes={name: {0: batch} for name in input_names},
                        dynamo=True,
                    )
                    session = onnxruntime.InferenceSession(
                        path, providers=["CPUExecutionProvider"]
                    )

                for batch_size in [1, 3]:
                    inputs = make_inputs(batch_size)
                    with torch.no_grad():
                        expected = model(*inputs)
                    actual = session.run(
                        None,
                        {name: x.numpy() for name, x in zip(input_names, inputs)},
                    )
                    for a, e in zip(actual, expected):
                        np.testing.assert_allclose(a, e.numpy(), rtol=1e-5, atol=1e-5)


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))

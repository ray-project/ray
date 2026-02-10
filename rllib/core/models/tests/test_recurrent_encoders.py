import itertools
import unittest

import numpy as np

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


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))

import unittest
import itertools

from ray.rllib.utils.framework import try_import_torch
from ray.rllib.core.models.configs import CNNEncoderConfig
from ray.rllib.models.utils import get_filter_config
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.core.models.base import ENCODER_OUT, STATE_IN, STATE_OUT

torch, nn = try_import_torch()


class TestTorchCNNEncoder(unittest.TestCase):
    def test_torch_cnn_encoder(self):

        # Input dims supported by RLlib via get_filter_config()
        inputs_dimss = [
            [480, 640, 3],
            [480, 640, 1],
            [240, 320, 3],
            [240, 320, 1],
            [96, 96, 3],
            [96, 96, 1],
            [84, 84, 3],
            [84, 84, 1],
            [42, 42, 3],
            [42, 42, 1],
            [10, 10, 3],
        ]

        filter_layer_activation = [None, "linear", "relu"]

        output_dims_configs = [[1], [100]]

        output_activations = filter_layer_activation

        for permutation in itertools.product(
            inputs_dimss,
            filter_layer_activation,
            output_activations,
            output_dims_configs,
        ):
            (
                inputs_dims,
                filter_layer_activation,
                output_activation,
                output_dims,
            ) = permutation

            filter_specifiers = get_filter_config(inputs_dims)

            print(
                f"Testing ...\n"
                f"inputs_dims: {inputs_dims}\n"
                f"filter_specifiers: {filter_specifiers}\n"
                f"filter_layer_activation: {filter_layer_activation}\n"
                f"output_activation: {output_activation}\n"
                f"output_dims: {output_dims}\n"
            )

            config = CNNEncoderConfig(
                input_dims=inputs_dims,
                filter_specifiers=filter_specifiers,
                filter_layer_activation=filter_layer_activation,
                output_activation=output_activation,
                output_dims=output_dims,
            )

            model = config.build(framework="torch")
            print(model)

            obs = torch.randn(1, *inputs_dims)
            seq_lens = torch.tensor([1])
            state = None

            outputs = model(
                {SampleBatch.OBS: obs, SampleBatch.SEQ_LENS: seq_lens, STATE_IN: state}
            )

            self.assertEqual(outputs[ENCODER_OUT].shape, (1, output_dims[0]))
            self.assertEqual(outputs[STATE_OUT], None)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))

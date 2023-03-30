import unittest
import itertools

from ray.rllib.core.models.base import ENCODER_OUT, STATE_IN, STATE_OUT
from ray.rllib.core.models.configs import CNNEncoderConfig
from ray.rllib.models.utils import get_filter_config
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.test_utils import framework_iterator

_, tf, _ = try_import_tf()
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
            [64, 64, 3],
            [64, 64, 1],
            [42, 42, 3],
            [42, 42, 1],
            [10, 10, 3],
        ]

        cnn_activations = [None, "linear", "relu"]

        output_dims_configs = [[1], [100]]

        output_activations = cnn_activations

        for permutation in itertools.product(
            inputs_dimss,
            cnn_activations,
            output_activations,
            output_dims_configs,
        ):
            (
                inputs_dims,
                cnn_activation,
                output_activation,
                output_dims,
            ) = permutation

            filter_specifiers = get_filter_config(inputs_dims)

            config = CNNEncoderConfig(
                input_dims=inputs_dims,
                cnn_filter_specifiers=filter_specifiers,
                cnn_activation=cnn_activation,
                output_dims=output_dims,
                output_activation=output_activation,
            )

            for fw in framework_iterator(frameworks=("tf2", "torch")):
                print(
                    f"Testing ...\n"
                    f"inputs_dims: {inputs_dims}\n"
                    f"filter_specifiers: {filter_specifiers}\n"
                    f"filter_layer_activation: {cnn_activation}\n"
                    f"output_activation: {output_activation}\n"
                    f"output_dims: {output_dims}\n"
                )
                model = config.build(framework=fw)
                print(model)

                # Pass a B=1 observation through the model.
                if fw == "tf2":
                    obs = tf.random.uniform([1] + inputs_dims)
                    seq_lens = tf.Variable([1])
                else:
                    obs = torch.randn(1, *inputs_dims)
                    seq_lens = torch.tensor([1])
                state = None

                outputs = model({
                    SampleBatch.OBS: obs,
                    SampleBatch.SEQ_LENS: seq_lens,
                    STATE_IN: state,
                })

                self.assertEqual(outputs[ENCODER_OUT].shape, (1, output_dims[0]))
                self.assertEqual(outputs[STATE_OUT], None)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))

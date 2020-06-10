import unittest
import numpy as np

from ray.rllib.models.tf.layers import \
    MultiHeadAttention, RelativeMultiHeadAttention
from ray.rllib.models.torch.modules.multi_head_attention import \
    TorchMultiHeadAttention
from ray.rllib.models.torch.modules.relative_multi_head_attention import \
    TorchRelativeMultiHeadAttention
from ray.rllib.utils.framework import try_import_torch, try_import_tf
from ray.rllib.models.torch.modules.gru_gate import GRUGate
from ray.rllib.utils.test_utils import framework_iterator
from ray.rllib.models.torch.attention_net import relative_position_embedding_torch
from ray.rllib.models.tf.attention_net import relative_position_embedding

torch, nn = try_import_torch()
tf = try_import_tf()


class TestModules(unittest.TestCase):
    """Tests various torch/modules and tf/layers required for AttentionNet"""

    def train_torch_model(self, model, inputs, outputs, num_epochs=250):
        """Convenience method that trains a Torch model for num_epochs epochs
            and tests whether loss decreased, as expected.

        Args:
            model (nn.Module): Torch model to be trained.
            inputs (torch.Tensor): Training data
            outputs (torch.Tensor): Training labels
            num_epochs (int): Number of epochs to train for
        """
        criterion = torch.nn.MSELoss(reduction='sum')
        optimizer = torch.optim.SGD(model.parameters(), lr=1e-4)

        # Check that the layer trains correctly
        for t in range(num_epochs):
            y_pred = model(inputs)
            # if t % 100 == 99:
            #    print(t, loss.item())

            if t == 1:
                init_loss = loss.item()
            loss = criterion(y_pred, outputs)

            optimizer.zero_grad()
            loss.backward()
            optimizer.step()

        final_loss = loss.item()

        # The final loss has decreased by a factor of 2, which tests
        # that the model is learning from the training data.
        self.assertLess(final_loss / init_loss, 0.5)

    def train_tf_model(self, model, inputs, outputs, num_epochs=250):
        """Convenience method that trains a Tensorflow model for num_epochs
            epochs and tests whether loss decreased, as expected.

        Args:
            model (tf.Model): Torch model to be trained.
            inputs (np.array): Training data
            outputs (np.array): Training labels
        """

        # Configure a model for mean-squared error loss.
        model.compile(
            optimizer='SGD',
            loss='mse',  # mean squared error
            metrics=['mae'])  # mean absolute error

        hist = model.fit(
            inputs, outputs, verbose=0, epochs=num_epochs,
            batch_size=32).history
        init_loss = hist['loss'][0]
        final_loss = hist['loss'][-1]

        self.assertLess(final_loss / init_loss, 0.5)

    def test_multi_head_attention(self):
        """Tests the MultiHeadAttention mechanism of Vaswani et al."""
        #B is batch size
        B = 1
        # D_in is attention dim, L is memory_tau
        L, D_in, D_out = 2, 32, 10

        for fw, sess in framework_iterator(
                frameworks=("tfe", "torch", "tf"), session=True):

            # Create a single attention layer with 2 heads
            if fw == 'torch':

                # Create random Tensors to hold inputs and outputs
                x = torch.randn(B, L, D_in)
                y = torch.randn(B, L, D_out)

                model = TorchRelativeMultiHeadAttention(
                    in_dim=D_in, out_dim=D_out, num_heads=2, head_dim=32)

                self.train_torch_model(model, x, y)

            else:  # framework is tensorflow or tensorflow-eager

                x = np.random.random((B, L, D_in))
                y = np.random.random((B, L, D_out))

                inputs = tf.keras.layers.Input(shape=(L, D_in))

                model = tf.keras.Sequential([
                    inputs,
                    MultiHeadAttention(
                        out_dim=D_out, num_heads=2, head_dim=32)
                ])
                self.train_tf_model(model, x, y)

    def test_relative_mha(self):
        """Tests the base transformer unit for TrXL, with a skip connection \
            wrapped around a MultiHeadAttention layer"""

        # Checks that torch and tf embedding matrices are the same
        assert relative_position_embedding(20, 15) == \
            relative_position_embedding_torch((20, 15))

        #B is batch size
        B = 1
        # D_in is attention dim, L is memory_tau
        L, D_in, D_out = 2, 32, 10

        for fw, sess in framework_iterator(
                frameworks=("tfe", "torch", "tf"), session=True):

            # Create a single attention layer with 2 heads
            if fw == 'torch':

                # Create random Tensors to hold inputs and outputs
                x = torch.randn(B, L, D_in)
                y = torch.randn(B, L, D_out)

                model = TorchRelativeMultiHeadAttention(
                    in_dim=D_in, out_dim=D_out, num_heads=2, head_dim=32)

                self.train_torch_model(model, x, y)

            else:  # framework is tensorflow or tensorflow-eager

                Phi = relative_position_embedding(
                    self.max_seq_len + self.memory_tau, D_in)

                x = np.random.random((B, L, D_in))
                y = np.random.random((B, L, D_out))

                inputs = tf.keras.layers.Input(shape=(L, D_in))

                model = tf.keras.Sequential([
                    inputs,
                    RelativeMultiHeadAttention(
                        out_dim=D_out, num_heads=2, head_dim=32)
                ])
                self.train_tf_model(model, x, y)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))

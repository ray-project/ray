import unittest
import numpy as np

from ray.rllib.models.tf.layers import \
    MultiHeadAttention
from ray.rllib.models.torch.modules.multi_head_attention import \
    TorchMultiHeadAttention
from ray.rllib.utils.framework import try_import_torch, try_import_tf
from ray.rllib.models.torch.modules.gru_gate import GRUGate
from ray.rllib.utils.test_utils import framework_iterator

torch, nn = try_import_torch()
tf = try_import_tf()


class TestModules(unittest.TestCase):
    """Tests models/torch/modules helper classes for attention net."""

    def torch_layer(self, model, inputs, outputs):
        """Model is a torch nn.Module. Input and output are randomly tensors"""

        # Train the model for a fixed number of timesteps, and then test that
        # loss decreased
        criterion = torch.nn.MSELoss(reduction='sum')
        optimizer = torch.optim.SGD(model.parameters(), lr=1e-4)

        # Check that the layer trains correctly
        for t in range(250):
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
        # that the model is learning.
        self.assertLess(final_loss / init_loss, 0.5)

    def tf_layer(self, model, inputs, outputs):
        """Model is a keras.Layer object, to be trained on fixed random data"""

        # Configure a model for mean-squared error loss.
        model.compile(
            optimizer='SGD',
            loss='mse',  # mean squared error
            metrics=['mae'])  # mean absolute error

        hist = model.fit(
            inputs, outputs, verbose=0, epochs=250, batch_size=32).history
        init_loss = hist['loss'][0]
        final_loss = hist['loss'][-1]

        self.assertLess(final_loss / init_loss, 0.5)

    def test_multi_head_attention(self):
        """Tests the MultiHeadAttention mechanism of Vaswani et al."""

        for fw, sess in framework_iterator(
            frameworks=("tfe", "torch", "tf"), session=True):

            # Create a single attention layer with 2 heads
            if fw == 'torch':
                B = 1
                L, D_in, D_out = 2, 32, 10

                # Create random Tensors to hold inputs and outputs
                x = torch.randn(B, L, D_in)
                y = torch.randn(B, L, D_out)

                model = TorchMultiHeadAttention(
                    in_dim=D_in, out_dim=D_out, num_heads=2, head_dim=32)

                self.torch_layer(model, x, y)

            else:  # framework is tensorflow or tensorflow-eager
                B = 1
                L, D_in, D_out = 2, 32, 10

                x = np.random.random((B, L, D_in))
                y = np.random.random((B, L, D_out))

                inputs = tf.keras.layers.Input(shape=(L, D_in))

                model = tf.keras.Sequential(
                    [inputs, MultiHeadAttention(out_dim=D_out, num_heads=2,
                                        head_dim=32)])
                self.tf_layer(model, x, y)

    def test_gru_gate(self):
        """Tests the MultiHeadAttention mechanism of Vaswani et al."""

        B = 1
        L, D_in, D_out = 64, 32, 10

        # Create random Tensors to hold inputs and outputs
        x = torch.randn(B, L, D_in)
        y = torch.randn(B, L, D_out)

        # Create a single attention layer with 2 heads
        #        model = GRUGate(input_shape= , init_bias=1)
        model = MultiHeadAttention(
            in_dim=D_in, out_dim=D_out, num_heads=2, head_dim=32)

        self.torch_layer(model, x, y)

if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))

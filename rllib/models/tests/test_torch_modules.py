import gym
import numpy as np
import unittest

from ray.rllib.models.tf.attention_net import relative_position_embedding

from ray.rllib.models.tf.layers import \
    MultiHeadAttention
from ray.rllib.models.torch.attention_net import relative_position_embedding \
    as relative_position_embedding_torch, GTrXLNet as TorchGTrXLNet
from ray.rllib.models.tf.attention_net import GTrXLNet
from ray.rllib.models.torch.modules.multi_head_attention import \
    MultiHeadAttention as TorchMultiHeadAttention
from ray.rllib.utils.framework import try_import_torch, try_import_tf
from ray.rllib.utils.test_utils import framework_iterator

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
            loss = criterion(y_pred, outputs)

            if t == 1:
                init_loss = loss.item()

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
            num_epochs (int): Number of training epochs
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
        # B is batch size
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

                model = TorchMultiHeadAttention(
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

    def test_attention_net(self):
        """Tests the GTrXL. Builds a full AttentionNet and checks
            that it trains"""

        # Checks that torch and tf embedding matrices are the same
        with tf.Session().as_default() as sess:
            assert np.allclose(
                relative_position_embedding(20, 15).eval(session=sess),
                relative_position_embedding_torch(20, 15).numpy())

        # B is batch size
        B = 1
        # D_in is attention dim, L is memory_tau
        L, D_in, D_out = 2, 16, 2

        for fw, sess in framework_iterator(
                frameworks=("tfe", "torch", "tf"), session=True):

            # Create a single attention layer with 2 heads
            if fw == 'torch':
                #continue
                # Create random Tensors to hold inputs and outputs
                x = torch.randn(B, L, D_in)
                y = torch.randn(B, L, D_out)

                value_labels = torch.randn(B, L, D_in)
                memory_labels = torch.randn(B, L, D_out)

                attention_net = TorchGTrXLNet(
                    observation_space=gym.spaces.Box(
                        low=float("-inf"), high=float("inf"), shape=(D_in,)),
                    action_space=gym.spaces.Discrete(2),
                    num_outputs=D_out,
                    model_config={"max_seq_len": 2},
                    name="TestTorchAttentionNet",
                    num_transformer_units=1,
                    attn_dim=D_in,
                    num_heads=2,
                    memory_tau=L,
                    head_dim=D_out,
                    ff_hidden_dim=16,
                    init_gate_bias=2.0)

                init_state = attention_net.get_initial_state()
                init_state = [np.expand_dims(s, 0) for s in init_state]

                self.train_torch_model(attention_net,
                                    [x] + init_state,
                                    [y, value_labels, memory_labels],
                                    num_epochs=25)

            else:  # Framework is tensorflow or tensorflow-eager.
                continue
                x = np.random.random((B, L, D_in))
                y = np.random.random((B, L, D_out))

                value_labels = np.random.random((B, L, 1))
                memory_labels = np.random.random((B, L, D_in))

                attention_net = GTrXLNet(
                    observation_space=gym.spaces.Box(
                        low=float("-inf"), high=float("inf"), shape=(D_in,)),
                    action_space=gym.spaces.Discrete(2),
                    num_outputs=D_out,
                    model_config={"max_seq_len": 2},
                    name="TestTFAttentionNet",
                    num_transformer_units=1,
                    attn_dim=D_in,
                    num_heads=2,
                    memory_tau=L,
                    head_dim=D_out,
                    ff_hidden_dim=16,
                    init_gate_bias=2.0)
                model = attention_net.trxl_model
                init_state = attention_net.get_initial_state()
                init_state = [np.expand_dims(s, 0) for s in init_state]

                self.train_tf_model(model,
                                    [x] + init_state,
                                    [y, value_labels, memory_labels],
                                    num_epochs=50)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))

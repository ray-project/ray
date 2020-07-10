import gym
import numpy as np
import unittest

from ray.rllib.models.tf.attention_net import relative_position_embedding, \
    GTrXLNet
from ray.rllib.models.tf.layers import MultiHeadAttention
from ray.rllib.models.torch.attention_net import relative_position_embedding \
    as relative_position_embedding_torch, GTrXLNet as TorchGTrXLNet
from ray.rllib.models.torch.modules.multi_head_attention import \
    MultiHeadAttention as TorchMultiHeadAttention
from ray.rllib.utils.framework import try_import_torch, try_import_tf
from ray.rllib.utils.test_utils import framework_iterator

torch, nn = try_import_torch()
tf1, tf, tfv = try_import_tf()


class TestModules(unittest.TestCase):
    """Tests various torch/modules and tf/layers required for AttentionNet"""

    def train_torch_full_model(self,
                               model,
                               inputs,
                               outputs,
                               num_epochs=250,
                               state=None,
                               seq_lens=None):
        """Convenience method that trains a Torch model for num_epochs epochs
            and tests whether loss decreased, as expected.

        Args:
            model (nn.Module): Torch model to be trained.
            inputs (torch.Tensor): Training data
            outputs (torch.Tensor): Training labels
            num_epochs (int): Number of epochs to train for
            state (torch.Tensor): Internal state of module
            seq_lens (torch.Tensor): Tensor of sequence lengths
        """

        criterion = torch.nn.MSELoss(reduction="sum")
        optimizer = torch.optim.Adam(model.parameters(), lr=3e-4)

        # Check that the layer trains correctly
        for t in range(num_epochs):
            y_pred = model(inputs, state, seq_lens)
            loss = criterion(y_pred[0], torch.squeeze(outputs[0]))

            if t % 10 == 1:
                print(t, loss.item())

            # if t == 0:
            #     init_loss = loss.item()

            optimizer.zero_grad()
            loss.backward()
            optimizer.step()

        # final_loss = loss.item()

        # The final loss has decreased, which tests
        # that the model is learning from the training data.
        # self.assertLess(final_loss / init_loss, 0.99)

    def train_torch_layer(self, model, inputs, outputs, num_epochs=250):
        """Convenience method that trains a Torch model for num_epochs epochs
            and tests whether loss decreased, as expected.

        Args:
            model (nn.Module): Torch model to be trained.
            inputs (torch.Tensor): Training data
            outputs (torch.Tensor): Training labels
            num_epochs (int): Number of epochs to train for
        """
        criterion = torch.nn.MSELoss(reduction="sum")
        optimizer = torch.optim.SGD(model.parameters(), lr=1e-4)

        # Check that the layer trains correctly
        for t in range(num_epochs):
            y_pred = model(inputs)
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

    def train_tf_model(self,
                       model,
                       inputs,
                       outputs,
                       num_epochs=250,
                       minibatch_size=32):
        """Convenience method that trains a Tensorflow model for num_epochs
            epochs and tests whether loss decreased, as expected.

        Args:
            model (tf.Model): Torch model to be trained.
            inputs (np.array): Training data
            outputs (np.array): Training labels
            num_epochs (int): Number of training epochs
            batch_size (int): Number of samples in each minibatch
        """

        # Configure a model for mean-squared error loss.
        model.compile(optimizer="SGD", loss="mse", metrics=["mae"])

        hist = model.fit(
            inputs,
            outputs,
            verbose=0,
            epochs=num_epochs,
            batch_size=minibatch_size).history
        init_loss = hist["loss"][0]
        final_loss = hist["loss"][-1]

        self.assertLess(final_loss / init_loss, 0.5)

    def test_multi_head_attention(self):
        """Tests the MultiHeadAttention mechanism of Vaswani et al."""
        # B is batch size
        B = 1
        # D_in is attention dim, L is memory_tau
        L, D_in, D_out = 2, 32, 10

        for fw, sess in framework_iterator(
                frameworks=("tfe", "torch", "tf"), session=True):
            # Create a single attention layer with 2 heads.
            if fw == "torch":

                # Create random Tensors to hold inputs and outputs
                x = torch.randn(B, L, D_in)
                y = torch.randn(B, L, D_out)

                model = TorchMultiHeadAttention(
                    in_dim=D_in, out_dim=D_out, num_heads=2, head_dim=32)

                self.train_torch_layer(model, x, y, num_epochs=500)

            # Framework is tensorflow or tensorflow-eager.
            else:
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
        """Tests the GTrXL.

        Builds a full AttentionNet and checks that it trains in a supervised
        setting."""

        # Checks that torch and tf embedding matrices are the same
        with tf1.Session().as_default() as sess:
            assert np.allclose(
                relative_position_embedding(20, 15).eval(session=sess),
                relative_position_embedding_torch(20, 15).numpy())

        # B is batch size
        B = 32
        # D_in is attention dim, L is memory_tau
        L, D_in, D_out = 2, 16, 2

        for fw, sess in framework_iterator(session=True):

            # Create a single attention layer with 2 heads
            if fw == "torch":
                # Create random Tensors to hold inputs and outputs
                x = torch.randn(B, L, D_in)
                y = torch.randn(B, L, D_out)

                value_labels = torch.randn(B, L, D_in)
                memory_labels = torch.randn(B, L, D_out)

                attention_net = TorchGTrXLNet(
                    observation_space=gym.spaces.Box(
                        low=float("-inf"), high=float("inf"), shape=(D_in, )),
                    action_space=gym.spaces.Discrete(D_out),
                    num_outputs=D_out,
                    model_config={"max_seq_len": 2},
                    name="TestTorchAttentionNet",
                    num_transformer_units=2,
                    attn_dim=D_in,
                    num_heads=2,
                    memory_tau=L,
                    head_dim=D_out,
                    ff_hidden_dim=16,
                    init_gate_bias=2.0)

                init_state = attention_net.get_initial_state()

                # Get initial state and add a batch dimension.
                init_state = [np.expand_dims(s, 0) for s in init_state]
                seq_lens_init = torch.full(size=(B, ), fill_value=L)

                # Torch implementation expects a formatted input_dict instead
                # of a numpy array as input.
                input_dict = {"obs": x}
                self.train_torch_full_model(
                    attention_net,
                    input_dict, [y, value_labels, memory_labels],
                    num_epochs=250,
                    state=init_state,
                    seq_lens=seq_lens_init)
            # Framework is tensorflow or tensorflow-eager.
            else:
                x = np.random.random((B, L, D_in))
                y = np.random.random((B, L, D_out))

                value_labels = np.random.random((B, L, 1))
                memory_labels = np.random.random((B, L, D_in))

                # We need to create (N-1) MLP labels for N transformer units
                mlp_labels = np.random.random((B, L, D_in))

                attention_net = GTrXLNet(
                    observation_space=gym.spaces.Box(
                        low=float("-inf"), high=float("inf"), shape=(D_in, )),
                    action_space=gym.spaces.Discrete(D_out),
                    num_outputs=D_out,
                    model_config={"max_seq_len": 2},
                    name="TestTFAttentionNet",
                    num_transformer_units=2,
                    attn_dim=D_in,
                    num_heads=2,
                    memory_tau=L,
                    head_dim=D_out,
                    ff_hidden_dim=16,
                    init_gate_bias=2.0)
                model = attention_net.trxl_model

                # Get initial state and add a batch dimension.
                init_state = attention_net.get_initial_state()
                init_state = [np.tile(s, (B, 1, 1)) for s in init_state]

                self.train_tf_model(
                    model, [x] + init_state,
                    [y, value_labels, memory_labels, mlp_labels],
                    num_epochs=200,
                    minibatch_size=B)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))

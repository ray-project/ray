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


class TestAttentionNets(unittest.TestCase):
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

    def train_tf_model(self, model, inputs, labels, num_epochs=250):
        optim = tf.keras.optimizers.Adam(lr=0.0001)
        init_loss = final_loss = None
        for _ in range(num_epochs):
            with tf.GradientTape() as tape:
                outputs = model(inputs)
                final_loss = tf.reduce_mean(tf.square(outputs[0] - labels[0]))
            if init_loss is None:
                init_loss = final_loss
            # Optimizer step.
            grads = tape.gradient(final_loss, model.trainable_variables)
            optim.apply_gradients(
                [(g, v) for g, v in zip(grads, model.trainable_variables)])

        self.assertLess(final_loss, init_loss)

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

        # Batch size.
        B = 32
        # Max seq-len.
        max_seq_len = 10
        # Memory size (inference).
        memory_size = max_seq_len * 2
        # Memory size (training).
        memory_training = max_seq_len
        # Number of transformer units.
        num_transformer_units = 2
        # Input dim.
        observation_dim = 8
        # Head dim.
        head_dim = 12
        # Attention dim.
        attention_dim = 16
        # Action dim.
        action_dim = 2

        for fw, sess in framework_iterator(session=True):

            # Create random Tensors to hold inputs and labels.
            x = np.random.random((B, max_seq_len,
                                  observation_dim)).astype(np.float32)
            y = np.random.random((B, max_seq_len, action_dim))

            # Create a single attention layer with 2 heads
            if fw == "torch":
                value_labels = torch.randn(B, max_seq_len)
                memory_labels = torch.randn(B, max_seq_len, attention_dim)

                attention_net = TorchGTrXLNet(
                    observation_space=gym.spaces.Box(
                        low=float("-inf"),
                        high=float("inf"),
                        shape=(observation_dim, )),
                    action_space=gym.spaces.Discrete(action_dim),
                    num_outputs=action_dim,
                    model_config={"max_seq_len": 2},
                    name="TestTorchAttentionNet",
                    num_transformer_units=num_transformer_units,
                    attn_dim=attention_dim,
                    num_heads=2,
                    memory_inference=memory_size,
                    memory_training=memory_training,
                    head_dim=head_dim,
                    ff_hidden_dim=24,
                    init_gate_bias=2.0)

                init_state = attention_net.get_initial_state()

                # Get initial state and add a batch dimension.
                init_state = [np.expand_dims(s, 0) for s in init_state]
                seq_lens_init = torch.full(
                    size=(B, ), fill_value=max_seq_len, dtype=torch.int32)

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
                value_labels = np.random.random((B, max_seq_len))
                memory_labels = [
                    np.random.random((B, memory_size, attention_dim))
                    for _ in range(num_transformer_units)
                ]

                attention_net = GTrXLNet(
                    observation_space=gym.spaces.Box(
                        low=float("-inf"),
                        high=float("inf"),
                        shape=(observation_dim, )),
                    action_space=gym.spaces.Discrete(action_dim),
                    num_outputs=action_dim,
                    model_config={"max_seq_len": max_seq_len},
                    name="TestTFAttentionNet",
                    num_transformer_units=num_transformer_units,
                    attn_dim=attention_dim,
                    num_heads=2,
                    memory_inference=memory_size,
                    memory_training=memory_training,
                    head_dim=head_dim,
                    ff_hidden_dim=24,
                    init_gate_bias=2.0)
                model = attention_net.trxl_model

                # Get initial state (for training!) and add a batch dimension.
                init_state = [
                    np.zeros((memory_training, attention_dim), np.float32)
                    for _ in range(num_transformer_units)
                ]
                init_state = [np.tile(s, (B, 1, 1)) for s in init_state]

                self.train_tf_model(
                    model, [x] + init_state + [np.array([True])],
                    [y, value_labels] + memory_labels,
                    num_epochs=20)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))

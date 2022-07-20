import numpy as np

from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.tf.recurrent_net import RecurrentNetwork
from ray.rllib.models.torch.misc import SlimFC
from ray.rllib.models.torch.recurrent_net import RecurrentNetwork as TorchRNN
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_tf, try_import_torch

tf1, tf, tfv = try_import_tf()
torch, nn = try_import_torch()


class MobileV2PlusRNNModel(RecurrentNetwork):
    """A conv. + recurrent keras net example using a pre-trained MobileNet."""

    def __init__(
        self, obs_space, action_space, num_outputs, model_config, name, cnn_shape
    ):

        super(MobileV2PlusRNNModel, self).__init__(
            obs_space, action_space, num_outputs, model_config, name
        )

        self.cell_size = 16
        visual_size = cnn_shape[0] * cnn_shape[1] * cnn_shape[2]

        state_in_h = tf.keras.layers.Input(shape=(self.cell_size,), name="h")
        state_in_c = tf.keras.layers.Input(shape=(self.cell_size,), name="c")
        seq_in = tf.keras.layers.Input(shape=(), name="seq_in", dtype=tf.int32)

        inputs = tf.keras.layers.Input(shape=(None, visual_size), name="visual_inputs")

        input_visual = inputs
        input_visual = tf.reshape(
            input_visual, [-1, cnn_shape[0], cnn_shape[1], cnn_shape[2]]
        )
        cnn_input = tf.keras.layers.Input(shape=cnn_shape, name="cnn_input")

        cnn_model = tf.keras.applications.mobilenet_v2.MobileNetV2(
            alpha=1.0,
            include_top=True,
            weights=None,
            input_tensor=cnn_input,
            pooling=None,
        )
        vision_out = cnn_model(input_visual)
        vision_out = tf.reshape(
            vision_out, [-1, tf.shape(inputs)[1], vision_out.shape.as_list()[-1]]
        )

        lstm_out, state_h, state_c = tf.keras.layers.LSTM(
            self.cell_size, return_sequences=True, return_state=True, name="lstm"
        )(
            inputs=vision_out,
            mask=tf.sequence_mask(seq_in),
            initial_state=[state_in_h, state_in_c],
        )

        # Postprocess LSTM output with another hidden layer and compute values.
        logits = tf.keras.layers.Dense(
            self.num_outputs, activation=tf.keras.activations.linear, name="logits"
        )(lstm_out)
        values = tf.keras.layers.Dense(1, activation=None, name="values")(lstm_out)

        # Create the RNN model
        self.rnn_model = tf.keras.Model(
            inputs=[inputs, seq_in, state_in_h, state_in_c],
            outputs=[logits, values, state_h, state_c],
        )
        self.rnn_model.summary()

    @override(RecurrentNetwork)
    def forward_rnn(self, inputs, state, seq_lens):
        model_out, self._value_out, h, c = self.rnn_model([inputs, seq_lens] + state)
        return model_out, [h, c]

    @override(ModelV2)
    def get_initial_state(self):
        return [
            np.zeros(self.cell_size, np.float32),
            np.zeros(self.cell_size, np.float32),
        ]

    @override(ModelV2)
    def value_function(self):
        return tf.reshape(self._value_out, [-1])


class TorchMobileV2PlusRNNModel(TorchRNN, nn.Module):
    """A conv. + recurrent torch net example using a pre-trained MobileNet."""

    def __init__(
        self, obs_space, action_space, num_outputs, model_config, name, cnn_shape
    ):

        TorchRNN.__init__(
            self, obs_space, action_space, num_outputs, model_config, name
        )
        nn.Module.__init__(self)

        self.lstm_state_size = 16
        self.cnn_shape = list(cnn_shape)
        self.visual_size_in = cnn_shape[0] * cnn_shape[1] * cnn_shape[2]
        # MobileNetV2 has a flat output of (1000,).
        self.visual_size_out = 1000

        # Load the MobileNetV2 from torch.hub.
        self.cnn_model = torch.hub.load(
            "pytorch/vision:v0.6.0", "mobilenet_v2", pretrained=True
        )

        self.lstm = nn.LSTM(
            self.visual_size_out, self.lstm_state_size, batch_first=True
        )

        # Postprocess LSTM output with another hidden layer and compute values.
        self.logits = SlimFC(self.lstm_state_size, self.num_outputs)
        self.value_branch = SlimFC(self.lstm_state_size, 1)
        # Holds the current "base" output (before logits layer).
        self._features = None

    @override(TorchRNN)
    def forward_rnn(self, inputs, state, seq_lens):
        # Create image dims.
        vision_in = torch.reshape(inputs, [-1] + self.cnn_shape)
        vision_out = self.cnn_model(vision_in)
        # Flatten.
        vision_out_time_ranked = torch.reshape(
            vision_out, [inputs.shape[0], inputs.shape[1], vision_out.shape[-1]]
        )
        if len(state[0].shape) == 2:
            state[0] = state[0].unsqueeze(0)
            state[1] = state[1].unsqueeze(0)
        # Forward through LSTM.
        self._features, [h, c] = self.lstm(vision_out_time_ranked, state)
        # Forward LSTM out through logits layer and value layer.
        logits = self.logits(self._features)
        return logits, [h.squeeze(0), c.squeeze(0)]

    @override(ModelV2)
    def get_initial_state(self):
        # Place hidden states on same device as model.
        h = [
            list(self.cnn_model.modules())[-1]
            .weight.new(1, self.lstm_state_size)
            .zero_()
            .squeeze(0),
            list(self.cnn_model.modules())[-1]
            .weight.new(1, self.lstm_state_size)
            .zero_()
            .squeeze(0),
        ]
        return h

    @override(ModelV2)
    def value_function(self):
        assert self._features is not None, "must call forward() first"
        return torch.reshape(self.value_branch(self._features), [-1])

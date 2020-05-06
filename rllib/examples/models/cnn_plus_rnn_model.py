import numpy as np

from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.tf.recurrent_tf_modelv2 import RecurrentTFModelV2
from ray.rllib.models.torch.misc import SlimFC
from ray.rllib.models.torch.recurrent_torch_model import RecurrentTorchModel
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_tf, try_import_torch


tf = try_import_tf()
torch, nn = try_import_torch()


class CNNPlusRNNModel(RecurrentTFModelV2):
    """A conv. + recurrent keras net example using a pre-trained MobileNet."""

    def __init__(self, obs_space, action_space, num_outputs, model_config,
                 name, cnn_shape):

        super(CNNPlusRNNModel, self).__init__(
            obs_space, action_space, num_outputs, model_config, name)

        self.cell_size = 16
        visual_size = cnn_shape[0] * cnn_shape[1] * cnn_shape[2]

        state_in_h = tf.keras.layers.Input(shape=(self.cell_size, ), name="h")
        state_in_c = tf.keras.layers.Input(shape=(self.cell_size, ), name="c")
        seq_in = tf.keras.layers.Input(shape=(), name="seq_in", dtype=tf.int32)

        inputs = tf.keras.layers.Input(
            shape=(None, visual_size), name="visual_inputs")

        input_visual = inputs
        input_visual = tf.reshape(
            input_visual, [-1, cnn_shape[0], cnn_shape[1], cnn_shape[2]])
        cnn_input = tf.keras.layers.Input(shape=cnn_shape, name="cnn_input")

        cnn_model = tf.keras.applications.mobilenet_v2.MobileNetV2(
            alpha=1.0,
            include_top=True,
            weights=None,
            input_tensor=cnn_input,
            pooling=None)
        vision_out = cnn_model(input_visual)
        vision_out = tf.reshape(
            vision_out,
            [-1, tf.shape(inputs)[1],
             vision_out.shape.as_list()[-1]])

        lstm_out, state_h, state_c = tf.keras.layers.LSTM(
            self.cell_size,
            return_sequences=True,
            return_state=True,
            name="lstm")(
                inputs=vision_out,
                mask=tf.sequence_mask(seq_in),
                initial_state=[state_in_h, state_in_c])

        # Postprocess LSTM output with another hidden layer and compute values.
        logits = tf.keras.layers.Dense(
            self.num_outputs,
            activation=tf.keras.activations.linear,
            name="logits")(lstm_out)
        values = tf.keras.layers.Dense(
            1, activation=None, name="values")(lstm_out)

        # Create the RNN model
        self.rnn_model = tf.keras.Model(
            inputs=[inputs, seq_in, state_in_h, state_in_c],
            outputs=[logits, values, state_h, state_c])
        self.register_variables(self.rnn_model.variables)
        self.rnn_model.summary()

    @override(RecurrentTFModelV2)
    def forward_rnn(self, inputs, state, seq_lens):
        model_out, self._value_out, h, c = self.rnn_model([inputs, seq_lens] +
                                                          state)
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


class TorchCNNPlusRNNModel(RecurrentTorchModel):
    """A conv. + recurrent torch net example using a pre-trained MobileNet."""

    def __init__(self, obs_space, action_space, num_outputs, model_config,
                 name, cnn_shape):

        super().__init__(obs_space, action_space, num_outputs,
                         model_config, name)

        self.cell_size = 16
        self.visual_size = cnn_shape[0] * cnn_shape[1] * cnn_shape[2]

        #state_in_h = tf.keras.layers.Input(shape=(self.cell_size, ), name="h")
        #state_in_c = tf.keras.layers.Input(shape=(self.cell_size, ), name="c")
        #seq_in = tf.keras.layers.Input(shape=(), name="seq_in", dtype=tf.int32)

        #inputs = tf.keras.layers.Input(
        #    shape=(None, visual_size), name="visual_inputs")

        #input_visual = inputs
        #input_visual = tf.reshape(
        #    input_visual, [-1, cnn_shape[0], cnn_shape[1], cnn_shape[2]])
        #cnn_input = tf.keras.layers.Input(shape=cnn_shape, name="cnn_input")

        self.cnn_model = torch.hub.load(
            "pytorch/vision:v0.6.0", "mobilenet_v2", pretrained=True)
        #cnn_model = tf.keras.applications.mobilenet_v2.MobileNetV2(
        #    alpha=1.0,
        #    include_top=True,
        #    weights=None,
        #    input_tensor=cnn_input,
        #    pooling=None)

        #vision_out = cnn_model(input_visual)
        #vision_out = tf.reshape(
        #    vision_out,
        #    [-1, tf.shape(inputs)[1],
        #     vision_out.shape.as_list()[-1]])

        self.lstm = nn.LSTM(
            self.visual_size, self.cell_size, batch_first=True)
        #lstm_out, state_h, state_c = tf.keras.layers.LSTM(
        #    self.cell_size,
        #    return_sequences=True,
        #    return_state=True,
        #    name="lstm")(
        #        inputs=vision_out,
        #        mask=tf.sequence_mask(seq_in),
        #        initial_state=[state_in_h, state_in_c])

        # Postprocess LSTM output with another hidden layer and compute values.
        self.logits = SlimFC(self.cell_size, self.num_outputs)  #(lstm_out)
        self.value_branch = SlimFC(self.cell_size, 1)  #(lstm_out)

        # Create the RNN model
        #self.rnn_model = tf.keras.Model(
        #    inputs=[inputs, seq_in, state_in_h, state_in_c],
        #    outputs=[logits, values, state_h, state_c])
        #self.register_variables(self.rnn_model.variables)
        #self.rnn_model.summary()

    @override(RecurrentTFModelV2)
    def forward_rnn(self, inputs, state, seq_lens):
        vision_out = self.cnn_model(inputs["obs"])
        # Flatten.
        vision_out_time_ranked = torch.reshape(
            vision_out, [-1, self.visual_size, vision_out.shape[-1]])
        # Forward through LSTM.
        lstm_out, [h, c] = self.lstm(
            vision_out_time_ranked,
            [torch.unsqueeze(state[0], 0), torch.unsqueeze(state[1], 0)])
        # Forward LSTM out through logits layer and value layer.
        logits = self.logits(lstm_out)
        self._value_out = self.value_branch(lstm_out)
        return logits, [h, c]

    @override(ModelV2)
    def get_initial_state(self):
        return [
            np.zeros(self.cell_size, np.float32),
            np.zeros(self.cell_size, np.float32),
        ]

    @override(ModelV2)
    def value_function(self):
        return torch.reshape(self._value_out, [-1])


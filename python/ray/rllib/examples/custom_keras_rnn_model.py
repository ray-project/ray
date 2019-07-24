"""Example of using a custom RNN keras model."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import argparse

import ray
from ray import tune
from ray.rllib.examples.cartpole_lstm import CartPoleStatelessEnv
from ray.rllib.models import ModelCatalog
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.tf.recurrent_tf_modelv2 import RecurrentTFModelV2
from ray.rllib.utils.annotations import override
from ray.rllib.utils import try_import_tf

tf = try_import_tf()

parser = argparse.ArgumentParser()
parser.add_argument("--run", type=str, default="PPO")
parser.add_argument("--stop", type=int, default=200)


class MyKerasRNN(RecurrentTFModelV2):
    """Example of using the Keras functional API to define a RNN model."""

    def __init__(self,
                 obs_space,
                 action_space,
                 num_outputs,
                 model_config,
                 name,
                 hiddens_size=256,
                 cell_size=64):
        super(MyKerasRNN, self).__init__(obs_space, action_space, num_outputs,
                                         model_config, name)
        self.cell_size = cell_size

        # Define input layers
        input_layer = tf.keras.layers.Input(
            shape=(None, obs_space.shape[0]), name="inputs")
        state_in_h = tf.keras.layers.Input(shape=(cell_size, ), name="h")
        state_in_c = tf.keras.layers.Input(shape=(cell_size, ), name="c")
        seq_in = tf.keras.layers.Input(shape=(), name="seq_in")

        # Preprocess observation with a hidden layer and send to LSTM cell
        dense1 = tf.keras.layers.Dense(
            hiddens_size, activation=tf.nn.relu, name="dense1")(input_layer)
        lstm_out, state_h, state_c = tf.keras.layers.LSTM(
            cell_size, return_sequences=True, return_state=True, name="lstm")(
                inputs=dense1,
                mask=tf.sequence_mask(seq_in),
                initial_state=[state_in_h, state_in_c])

        # Postprocess LSTM output with another hidden layer and compute values
        dense2 = tf.keras.layers.Dense(
            hiddens_size, activation=tf.nn.relu, name="dense2")(lstm_out)
        logits = tf.keras.layers.Dense(
            self.num_outputs,
            activation=tf.keras.activations.linear,
            name="logits")(dense2)
        values = tf.keras.layers.Dense(
            1, activation=None, name="values")(dense2)

        # Create the RNN model
        self.rnn_model = tf.keras.Model(
            inputs=[input_layer, seq_in, state_in_h, state_in_c],
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


if __name__ == "__main__":
    ray.init()
    args = parser.parse_args()
    ModelCatalog.register_custom_model("rnn", MyKerasRNN)
    tune.run(
        args.run,
        stop={"episode_reward_mean": args.stop},
        config={
            "env": CartPoleStatelessEnv,
            "num_envs_per_worker": 4,
            "num_sgd_iter": 3,
            "vf_loss_coeff": 1e-4,
            "model": {
                "custom_model": "rnn",
                "max_seq_len": 7,
            },
        })

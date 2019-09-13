"""Example of using a custom RNN keras model."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import gym
from gym.spaces import Discrete
import numpy as np
import random
import argparse

import ray
from ray import tune
from ray.tune.registry import register_env
from ray.rllib.models import ModelCatalog
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.tf.recurrent_tf_modelv2 import RecurrentTFModelV2
from ray.rllib.utils.annotations import override
from ray.rllib.utils import try_import_tf

tf = try_import_tf()

parser = argparse.ArgumentParser()
parser.add_argument("--run", type=str, default="PPO")
parser.add_argument("--env", type=str, default="RepeatAfterMeEnv")
parser.add_argument("--stop", type=int, default=90)


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
        logits = tf.keras.layers.Dense(
            self.num_outputs,
            activation=tf.keras.activations.linear,
            name="logits")(lstm_out)
        values = tf.keras.layers.Dense(
            1, activation=None, name="values")(lstm_out)

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


class RepeatInitialEnv(gym.Env):
    """Simple env in which the policy learns to repeat the initial observation
    seen at timestep 0."""

    def __init__(self):
        self.observation_space = Discrete(2)
        self.action_space = Discrete(2)
        self.token = None
        self.num_steps = 0

    def reset(self):
        self.token = random.choice([0, 1])
        self.num_steps = 0
        return self.token

    def step(self, action):
        if action == self.token:
            reward = 1
        else:
            reward = -1
        self.num_steps += 1
        done = self.num_steps > 100
        return 0, reward, done, {}


class RepeatAfterMeEnv(gym.Env):
    """Simple env in which the policy learns to repeat a previous observation
    token after a given delay."""

    def __init__(self, config):
        self.observation_space = Discrete(2)
        self.action_space = Discrete(2)
        self.delay = config["repeat_delay"]
        assert self.delay >= 1, "delay must be at least 1"
        self.history = []

    def reset(self):
        self.history = [0] * self.delay
        return self._next_obs()

    def step(self, action):
        if action == self.history[-(1 + self.delay)]:
            reward = 1
        else:
            reward = -1
        done = len(self.history) > 100
        return self._next_obs(), reward, done, {}

    def _next_obs(self):
        token = random.choice([0, 1])
        self.history.append(token)
        return token


if __name__ == "__main__":
    ray.init()
    args = parser.parse_args()
    ModelCatalog.register_custom_model("rnn", MyKerasRNN)
    register_env("RepeatAfterMeEnv", lambda c: RepeatAfterMeEnv(c))
    register_env("RepeatInitialEnv", lambda _: RepeatInitialEnv())
    tune.run(
        args.run,
        stop={"episode_reward_mean": args.stop},
        config={
            "env": args.env,
            "env_config": {
                "repeat_delay": 2,
            },
            "gamma": 0.9,
            "num_workers": 0,
            "num_envs_per_worker": 20,
            "entropy_coeff": 0.001,
            "num_sgd_iter": 5,
            "vf_loss_coeff": 1e-5,
            "model": {
                "custom_model": "rnn",
                "max_seq_len": 20,
            },
        })

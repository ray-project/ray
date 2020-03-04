import gym

import numpy as np

from rllib.models.tf import attention
from rllib.models.tf import recurrent_tf_modelv2
from rllib.utils import try_import_tf
from rllib.examples.custom_keras_rnn_model import RepeatInitialEnv

tf = try_import_tf()


class LookAndPush(gym.Env):
    def __init__(self, env_config):
        del env_config
        self.action_space = gym.spaces.Discrete(2)
        self.observation_space = gym.spaces.Discrete(5)
        self._state = None
        self._case = None

    def reset(self):
        self._state = np.array([2])
        self._case = np.random.choice(2)
        return self._state

    def step(self, action):
        assert self.action_space.contains(action)

        if self._state[0] == 4:
            if action and self._case:
                return self._state, 10., True, {}
            else:
                return self._state, -10, False, {}
        else:
            if action:
                if self._state[0] == 0:
                    self._state += 2
                else:
                    self._state += 1
            elif self._state[0] == 2:
                self._state[0] = self._case

        return self._state, -1, False, {}


# make custom version of samplebatch to handle previous
class GRUTrXL(recurrent_tf_modelv2):

    def __init__(self, obs_space, action_space, num_outputs, model_config,
                 name):
        self.model = attention.make_GRU_TrXL(
            seq_length=model_config["seq_length"],
            num_layers=model_config["num_layers"],
            attn_dim=model_config["attn_dim"],
            num_heads=model_config["num_heads"],
            head_dim=model_config["head_dim"],
            ff_hidden_dim=model_config["ff_hidden_dim"],
        )
        # Postprocess TrXL output with another hidden layer and compute values
        self.logits_layer = tf.keras.layers.Dense(
            self.num_outputs,
            activation=tf.keras.activations.linear,
            name="logits")
        self.value_layer = tf.keras.layers.Dense(
            1, activation=None, name="values")

        super(GRUTrXL, self).__init__(self, obs_space, action_space,
                                      num_outputs, model_config, name)

    def forward_rnn(self, inputs, state, seq_lens):
        obs = inputs["obs"]

        if inputs.shape[1] == 1:
            state = tf.roll(state, -1, axis=0)
            state[:, -1] = obs  # this probably isn't supported
            obs = state

        self.trxl_out = self.model(obs)
        self._value_out = self.value_layer(self.trxl_out)

        return self.logits_layer(self.trxl_out), state

    def value_function(self):
        return tf.reshape(self._value_out, [-1])

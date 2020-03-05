import argparse

import gym

import numpy as np

import ray
from ray import tune

from ray.tune import registry

from ray.rllib import models
from ray.rllib.utils import try_import_tf
from ray.rllib.models.tf import attention
from ray.rllib.models.tf import recurrent_tf_modelv2
from ray.rllib.examples.custom_keras_rnn_model import RepeatAfterMeEnv
from ray.rllib.examples.custom_keras_rnn_model import RepeatInitialEnv

tf = try_import_tf()

parser = argparse.ArgumentParser()
parser.add_argument("--run", type=str, default="PPO")
parser.add_argument("--env", type=str, default="RepeatAfterMeEnv")
parser.add_argument("--stop", type=int, default=90)
parser.add_argument("--num-cpus", type=int, default=0)


class OneHot(gym.Wrapper):

    def __init__(self, env):
        super(OneHot, self).__init__(env)
        self.observation_space = gym.spaces.Box(0., 1.,
                                                (env.observation_space.n,))

    def reset(self, **kwargs):
        obs = self.env.reset(**kwargs)
        return self._encode_obs(obs)

    def step(self, action):
        obs, reward, done, info = self.env.step(action)
        return self._encode_obs(obs), reward, done, info

    def _encode_obs(self, obs):
        new_obs = np.ones(self.env.observation_space.n)
        new_obs[obs] = 1.0
        return new_obs


class LookAndPush(gym.Env):
    def __init__(self):
        self.action_space = gym.spaces.Discrete(2)
        self.observation_space = gym.spaces.Discrete(5)
        self._state = None
        self._case = None

    def reset(self):
        self._state = 2
        self._case = np.random.choice(2)
        return self._state

    def step(self, action):
        assert self.action_space.contains(action)

        if self._state == 4:
            if action and self._case:
                return self._state, 10., True, {}
            else:
                return self._state, -10, True, {}
        else:
            if action:
                if self._state == 0:
                    self._state = 2
                else:
                    self._state += 1
            elif self._state == 2:
                self._state = self._case

        return self._state, -1, False, {}


class GRUTrXL(recurrent_tf_modelv2.RecurrentTFModelV2):

    def __init__(self, obs_space, action_space, num_outputs, model_config,
                 name):
        super(GRUTrXL, self).__init__(obs_space, action_space, num_outputs,
                                      model_config, name)
        self.max_seq_len = model_config["max_seq_len"]
        self.obs_dim = obs_space.shape[0]
        input_layer = tf.keras.layers.Input(
            shape=(self.max_seq_len, obs_space.shape[0]),
            name="inputs",
        )

        trxl_out = attention.make_GRU_TrXL(
            seq_length=model_config["max_seq_len"],
            num_layers=model_config["custom_options"]["num_layers"],
            attn_dim=model_config["custom_options"]["attn_dim"],
            num_heads=model_config["custom_options"]["num_heads"],
            head_dim=model_config["custom_options"]["head_dim"],
            ff_hidden_dim=model_config["custom_options"]["ff_hidden_dim"],
        )(input_layer)

        # Postprocess TrXL output with another hidden layer and compute values
        logits = tf.keras.layers.Dense(
            self.num_outputs,
            activation=tf.keras.activations.linear,
            name="logits")(trxl_out)
        values_out = tf.keras.layers.Dense(
            1, activation=None, name="values")(trxl_out)

        self.trxl_model = tf.keras.Model(
            inputs=[input_layer],
            outputs=[logits, values_out],
        )
        self.register_variables(self.trxl_model.variables)
        self.trxl_model.summary()

    def forward_rnn(self, inputs, state, seq_lens):
        state = state[0]

        # We assume state is the history of recent observations and append
        # the current inputs to the end and only keep the most recent (up to
        # max_seq_len). This allows us to deal with timestep-wise inference
        # and full sequence training with the same logic.
        state = tf.concat((state, inputs), axis=1)[:, -self.max_seq_len:]
        logits, self._value_out = self.trxl_model(state)

        in_T = tf.shape(inputs)[1]
        logits = logits[:, -in_T:]
        self._value_out = self._value_out[:, -in_T:]

        return logits, [state]

    def get_initial_state(self):
        return [np.zeros((self.max_seq_len, self.obs_dim), np.float32)]

    def value_function(self):
        return tf.reshape(self._value_out, [-1])


if __name__ == "__main__":
    args = parser.parse_args()
    ray.init(num_cpus=args.num_cpus or None)
    models.ModelCatalog.register_custom_model("trxl", GRUTrXL)
    registry.register_env("RepeatAfterMeEnv", lambda c: RepeatAfterMeEnv(c))
    registry.register_env("RepeatInitialEnv", lambda _: RepeatInitialEnv())
    registry.register_env("LookAndPush", lambda _: OneHot(LookAndPush()))
    tune.run(
        args.run,
        stop={"episode_reward_mean": args.stop},
        config={
            "env": args.env,
            "env_config": {
                "repeat_delay": 2,
            },
            "gamma": 0.99,
            "num_workers": 0,
            "num_envs_per_worker": 20,
            "entropy_coeff": 0.001,
            "num_sgd_iter": 5,
            "vf_loss_coeff": 1e-5,
            "model": {
                "custom_model": "trxl",
                "max_seq_len": 10,
                "custom_options": {
                    "num_layers": 1,
                    "attn_dim": 10,
                    "num_heads": 1,
                    "head_dim": 10,
                    "ff_hidden_dim": 20,
                },
            },
        })

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
from ray.rllib.examples.custom_keras_rnn_model import RepeatInitialEnv

tf = try_import_tf()

parser = argparse.ArgumentParser()
parser.add_argument("--run", type=str, default="PPO")
parser.add_argument("--env", type=str, default="RepeatInitialEnv")
parser.add_argument("--stop", type=int, default=90)
parser.add_argument("--num-cpus", type=int, default=0)


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


if __name__ == "__main__":
    args = parser.parse_args()
    ray.init(num_cpus=args.num_cpus or None)
    models.ModelCatalog.register_custom_model("trxl", GRUTrXL)
    registry.register_env("RepeatInitialEnv", lambda _: RepeatInitialEnv())
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
                "custom_model": "trxl",
            },
        })

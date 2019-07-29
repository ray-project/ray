"""Example of specifying an autoregressive action distribution."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import gym
from gym.spaces import Discrete, Tuple
import argparse
import random

import ray
from ray import tune
from ray.rllib.models import ModelCatalog
from ray.rllib.models.tf.tf_action_dist import Categorical, ActionDistribution
from ray.rllib.models.tf.misc import normc_initializer
from ray.rllib.models.tf.tf_modelv2 import TFModelV2
from ray.rllib.policy.policy import TupleActions
from ray.rllib.utils import try_import_tf

tf = try_import_tf()

parser = argparse.ArgumentParser()
parser.add_argument("--run", type=str, default="PPO")
parser.add_argument("--stop", type=int, default=200)


class EmitCorrelatedActionsEnv(gym.Env):
    """Simple env in which the policy has to emit a tuple of equal actions.

    However, the policy is penalized for emitting the exact same action each
    time, so it has to learn P(a1) ~ random, then a2 = a1.

    The best score would be ~200 reward."""

    def __init__(self, _):
        self.observation_space = Discrete(2)
        self.action_space = Tuple([Discrete(2), Discrete(2)])

    def reset(self):
        self.t = 0
        self.last = random.choice([0, 1])
        return self.last

    def step(self, action):
        self.t += 1
        a1, a2 = action
        reward = 0
        if a1 == self.last:
            reward += 5
        # encourage correlation between a1 and a2
        if a1 == a2:
            reward += 5
        done = self.t > 20
        self.last = random.choice([0, 1])
        return self.last, reward, done, {}


def make_binary_autoregressive_output(action_model):
    """Returns an autoregressive ActionDistribution class for two outputs.

    Arguments:
        action_model: Keras model that takes [context, a1_sample] and returns
            logits for a1 and a2.
    """

    class AutoregressiveOutput(ActionDistribution):
        def sample(self):
            a1_dist = self._a1_distribution()
            a1 = a1_dist.sample()
            a2_dist = self._a2_distribution(a1)
            a2 = a2_dist.sample()
            self._action_prob = a1_dist.logp(a1) + a2_dist.logp(a2)
            return TupleActions([a1, a2])

        def sampled_action_prob(self):
            return self._action_prob

        def logp(self, actions):
            a1, a2 = actions[:, 0], actions[:, 1]
            a1_vec = tf.expand_dims(tf.cast(a1, tf.float32), 1)
            a1_logits, a2_logits = action_model([self.inputs, a1_vec])
            return (Categorical(a1_logits).logp(a1) +
                    Categorical(a2_logits).logp(a2))

        def entropy(self):
            a1_dist = self._a1_distribution()
            a2_dist = self._a2_distribution(a1_dist.sample())
            return a1_dist.entropy() + a2_dist.entropy()

        def kl(self, other):
            # TODO: implement this properly
            return tf.zeros_like(self.entropy())

        def _a1_distribution(self):
            BATCH = tf.shape(self.inputs)[0]
            a1_logits, _ = action_model([self.inputs, tf.zeros((BATCH, 1))])
            a1_dist = Categorical(a1_logits)
            return a1_dist

        def _a2_distribution(self, a1):
            a1_vec = tf.expand_dims(tf.cast(a1, tf.float32), 1)
            _, a2_logits = action_model([self.inputs, a1_vec])
            a2_dist = Categorical(a2_logits)
            return a2_dist

    return AutoregressiveOutput


class AutoregressiveActionsModel(TFModelV2):
    """Custom autoregressive model for policy gradient algorithms."""

    def __init__(self, obs_space, action_space, num_outputs, model_config,
                 name):
        super(AutoregressiveActionsModel, self).__init__(
            obs_space, action_space, num_outputs, model_config, name)
        # Inputs
        obs_input = tf.keras.layers.Input(
            shape=obs_space.shape, name="obs_input")
        a1_input = tf.keras.layers.Input(shape=(1, ), name="a1_input")
        # TODO(ekl) the context should be allowed to have a different size from
        # num_outputs. This currently doesn't work since RLlib checks the
        # model output size is equal to num_outputs.
        ctx_input = tf.keras.layers.Input(
            shape=(num_outputs, ), name="ctx_input")

        # Shared hidden layer
        context = tf.keras.layers.Dense(
            num_outputs,
            name="hidden",
            activation=tf.nn.tanh,
            kernel_initializer=normc_initializer(1.0))(obs_input)

        # V(s)
        value_out = tf.keras.layers.Dense(
            1,
            name="value_out",
            activation=None,
            kernel_initializer=normc_initializer(0.01))(context)

        # P(a1)
        a1_logits = tf.keras.layers.Dense(
            2,
            name="a1_logits",
            activation=None,
            kernel_initializer=normc_initializer(0.01))(ctx_input)

        # P(a2 | a1) -- note this doesn't include obs for example purposes,
        # which forces the model to learn a2 without knowing the obs. In
        # practice you'll want to use a Concat layer here so that a2 can be
        # conditioned on both the obs and a1.
        a2_hidden = tf.keras.layers.Dense(
            16,
            name="a2_hidden",
            activation=tf.nn.tanh,
            kernel_initializer=normc_initializer(1.0))(a1_input)
        a2_logits = tf.keras.layers.Dense(
            2,
            name="a2_logits",
            activation=None,
            kernel_initializer=normc_initializer(0.01))(a2_hidden)

        # Base layers
        self.base_model = tf.keras.Model(obs_input, [context, value_out])
        self.register_variables(self.base_model.variables)
        self.base_model.summary()

        # Autoregressive action sampler
        self.action_model = tf.keras.Model([ctx_input, a1_input],
                                           [a1_logits, a2_logits])
        self.action_model.summary()
        self.register_variables(self.action_model.variables)

    def forward(self, input_dict, state, seq_lens):
        context, self._value_out = self.base_model(input_dict["obs"])
        return context, state

    def value_function(self):
        return tf.reshape(self._value_out, [-1])

    def override_action_distribution(self):
        return make_binary_autoregressive_output(self.action_model)


if __name__ == "__main__":
    ray.init()
    args = parser.parse_args()
    ModelCatalog.register_custom_model("autoregressive_model",
                                       AutoregressiveActionsModel)
    tune.run(
        args.run,
        stop={"episode_reward_mean": args.stop},
        config={
            "env": EmitCorrelatedActionsEnv,
            "train_batch_size": 10,
            "sample_batch_size": 10,
            "gamma": 0.5,
            "num_gpus": 0,
            "model": {
                "custom_model": "autoregressive_model",
            },
        })

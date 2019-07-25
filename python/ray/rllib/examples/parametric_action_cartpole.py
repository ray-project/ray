"""Example of handling variable length and/or parametric action spaces.

This is a toy example of the action-embedding based approach for handling large
discrete action spaces (potentially infinite in size), similar to this:

    https://neuro.cs.ut.ee/the-use-of-embeddings-in-openai-five/

This currently works with RLlib's policy gradient style algorithms
(e.g., PG, PPO, IMPALA, A2C) and also DQN.

Note that since the model outputs now include "-inf" tf.float32.min
values, not all algorithm options are supported at the moment. For example,
algorithms might crash if they don't properly ignore the -inf action scores.
Working configurations are given below.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import random
import numpy as np
import gym
from gym.spaces import Box, Discrete, Dict

import ray
from ray import tune
from ray.rllib.agents.dqn.distributional_q_model import DistributionalQModel
from ray.rllib.models import ModelCatalog
from ray.rllib.models.tf.fcnet_v2 import FullyConnectedNetwork
from ray.rllib.models.tf.tf_modelv2 import TFModelV2
from ray.tune.registry import register_env
from ray.rllib.utils import try_import_tf

tf = try_import_tf()

parser = argparse.ArgumentParser()
parser.add_argument("--stop", type=int, default=200)
parser.add_argument("--run", type=str, default="PPO")


class ParametricActionCartpole(gym.Env):
    """Parametric action version of CartPole.

    In this env there are only ever two valid actions, but we pretend there are
    actually up to `max_avail_actions` actions that can be taken, and the two
    valid actions are randomly hidden among this set.

    At each step, we emit a dict of:
        - the actual cart observation
        - a mask of valid actions (e.g., [0, 0, 1, 0, 0, 1] for 6 max avail)
        - the list of action embeddings (w/ zeroes for invalid actions) (e.g.,
            [[0, 0],
             [0, 0],
             [-0.2322, -0.2569],
             [0, 0],
             [0, 0],
             [0.7878, 1.2297]] for max_avail_actions=6)

    In a real environment, the actions embeddings would be larger than two
    units of course, and also there would be a variable number of valid actions
    per step instead of always [LEFT, RIGHT].
    """

    def __init__(self, max_avail_actions):
        # Use simple random 2-unit action embeddings for [LEFT, RIGHT]
        self.left_action_embed = np.random.randn(2)
        self.right_action_embed = np.random.randn(2)
        self.action_space = Discrete(max_avail_actions)
        self.wrapped = gym.make("CartPole-v0")
        self.observation_space = Dict({
            "action_mask": Box(0, 1, shape=(max_avail_actions, )),
            "avail_actions": Box(-10, 10, shape=(max_avail_actions, 2)),
            "cart": self.wrapped.observation_space,
        })

    def update_avail_actions(self):
        self.action_assignments = np.array([[0., 0.]] * self.action_space.n)
        self.action_mask = np.array([0.] * self.action_space.n)
        self.left_idx, self.right_idx = random.sample(
            range(self.action_space.n), 2)
        self.action_assignments[self.left_idx] = self.left_action_embed
        self.action_assignments[self.right_idx] = self.right_action_embed
        self.action_mask[self.left_idx] = 1
        self.action_mask[self.right_idx] = 1

    def reset(self):
        self.update_avail_actions()
        return {
            "action_mask": self.action_mask,
            "avail_actions": self.action_assignments,
            "cart": self.wrapped.reset(),
        }

    def step(self, action):
        if action == self.left_idx:
            actual_action = 0
        elif action == self.right_idx:
            actual_action = 1
        else:
            raise ValueError(
                "Chosen action was not one of the non-zero action embeddings",
                action, self.action_assignments, self.action_mask,
                self.left_idx, self.right_idx)
        orig_obs, rew, done, info = self.wrapped.step(actual_action)
        self.update_avail_actions()
        obs = {
            "action_mask": self.action_mask,
            "avail_actions": self.action_assignments,
            "cart": orig_obs,
        }
        return obs, rew, done, info


class ParametricActionsModel(DistributionalQModel, TFModelV2):
    """Parametric action model that handles the dot product and masking.

    This assumes the outputs are logits for a single Categorical action dist.
    Getting this to work with a more complex output (e.g., if the action space
    is a tuple of several distributions) is also possible but left as an
    exercise to the reader.
    """

    def __init__(self,
                 obs_space,
                 action_space,
                 num_outputs,
                 model_config,
                 name,
                 true_obs_shape=(4, ),
                 action_embed_size=2,
                 **kw):
        super(ParametricActionsModel, self).__init__(
            obs_space, action_space, num_outputs, model_config, name, **kw)
        self.action_embed_model = FullyConnectedNetwork(
            Box(-1, 1, shape=true_obs_shape), action_space, action_embed_size,
            model_config, name + "_action_embed")
        self.register_variables(self.action_embed_model.variables())

    def forward(self, input_dict, state, seq_lens):
        # Extract the available actions tensor from the observation.
        avail_actions = input_dict["obs"]["avail_actions"]
        action_mask = input_dict["obs"]["action_mask"]

        # Compute the predicted action embedding
        action_embed, _ = self.action_embed_model({
            "obs": input_dict["obs"]["cart"]
        })

        # Expand the model output to [BATCH, 1, EMBED_SIZE]. Note that the
        # avail actions tensor is of shape [BATCH, MAX_ACTIONS, EMBED_SIZE].
        intent_vector = tf.expand_dims(action_embed, 1)

        # Batch dot product => shape of logits is [BATCH, MAX_ACTIONS].
        action_logits = tf.reduce_sum(avail_actions * intent_vector, axis=2)

        # Mask out invalid actions (use tf.float32.min for stability)
        inf_mask = tf.maximum(tf.log(action_mask), tf.float32.min)
        return action_logits + inf_mask, state

    def value_function(self):
        return self.action_embed_model.value_function()


if __name__ == "__main__":
    args = parser.parse_args()
    ray.init()

    ModelCatalog.register_custom_model("pa_model", ParametricActionsModel)
    register_env("pa_cartpole", lambda _: ParametricActionCartpole(10))
    if args.run == "DQN":
        cfg = {
            # TODO(ekl) we need to set these to prevent the masked values
            # from being further processed in DistributionalQModel, which
            # would mess up the masking. It is possible to support these if we
            # defined a a custom DistributionalQModel that is aware of masking.
            "hiddens": [],
            "dueling": False,
        }
    else:
        cfg = {}
    tune.run(
        args.run,
        stop={
            "episode_reward_mean": args.stop,
        },
        config=dict({
            "env": "pa_cartpole",
            "model": {
                "custom_model": "pa_model",
            },
            "num_workers": 0,
        }, **cfg),
    )

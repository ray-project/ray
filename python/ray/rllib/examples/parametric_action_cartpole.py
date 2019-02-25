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
import tensorflow as tf
import tensorflow.contrib.slim as slim

import ray
from ray.rllib.models import Model, ModelCatalog
from ray.rllib.models.misc import normc_initializer
from ray.tune import run_experiments
from ray.tune.registry import register_env

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
            "avail_actions": Box(-1, 1, shape=(max_avail_actions, 2)),
            "cart": self.wrapped.observation_space,
        })

    def update_avail_actions(self):
        self.action_assignments = [[0, 0]] * self.action_space.n
        self.action_mask = [0] * self.action_space.n
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


class ParametricActionsModel(Model):
    """Parametric action model that handles the dot product and masking.

    This assumes the outputs are logits for a single Categorical action dist.
    Getting this to work with a more complex output (e.g., if the action space
    is a tuple of several distributions) is also possible but left as an
    exercise to the reader.
    """

    def _build_layers_v2(self, input_dict, num_outputs, options):
        # Extract the available actions tensor from the observation.
        avail_actions = input_dict["obs"]["avail_actions"]
        action_mask = input_dict["obs"]["action_mask"]
        action_embed_size = avail_actions.shape[2].value
        if num_outputs != avail_actions.shape[1].value:
            raise ValueError(
                "This model assumes num outputs is equal to max avail actions",
                num_outputs, avail_actions)

        # Standard FC net component.
        last_layer = input_dict["obs"]["cart"]
        hiddens = [256, 256]
        for i, size in enumerate(hiddens):
            label = "fc{}".format(i)
            last_layer = slim.fully_connected(
                last_layer,
                size,
                weights_initializer=normc_initializer(1.0),
                activation_fn=tf.nn.tanh,
                scope=label)
        output = slim.fully_connected(
            last_layer,
            action_embed_size,
            weights_initializer=normc_initializer(0.01),
            activation_fn=None,
            scope="fc_out")

        # Expand the model output to [BATCH, 1, EMBED_SIZE]. Note that the
        # avail actions tensor is of shape [BATCH, MAX_ACTIONS, EMBED_SIZE].
        intent_vector = tf.expand_dims(output, 1)

        # Batch dot product => shape of logits is [BATCH, MAX_ACTIONS].
        action_logits = tf.reduce_sum(avail_actions * intent_vector, axis=2)

        # Mask out invalid actions (use tf.float32.min for stability)
        inf_mask = tf.maximum(tf.log(action_mask), tf.float32.min)
        masked_logits = inf_mask + action_logits

        return masked_logits, last_layer


if __name__ == "__main__":
    args = parser.parse_args()
    ray.init()

    ModelCatalog.register_custom_model("pa_model", ParametricActionsModel)
    register_env("pa_cartpole", lambda _: ParametricActionCartpole(10))
    if args.run == "PPO":
        cfg = {
            "observation_filter": "NoFilter",  # don't filter the action list
            "vf_share_layers": True,  # don't create duplicate value model
        }
    elif args.run == "DQN":
        cfg = {
            "hiddens": [],  # important: don't postprocess the action scores
        }
    else:
        cfg = {}  # PG, IMPALA, A2C, etc.
    run_experiments({
        "parametric_cartpole": {
            "run": args.run,
            "env": "pa_cartpole",
            "stop": {
                "episode_reward_mean": args.stop,
            },
            "config": dict({
                "model": {
                    "custom_model": "pa_model",
                },
                "num_workers": 0,
            }, **cfg),
        },
    })

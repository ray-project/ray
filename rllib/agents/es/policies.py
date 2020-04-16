# Code in this file is copied and adapted from
# https://github.com/openai/evolution-strategies-starter.

import gym
import logging
import numpy as np
import tree

import ray
import ray.experimental.tf_utils
from ray.rllib.evaluation.sampler import unbatch_actions
from ray.rllib.models import ModelCatalog
from ray.rllib.utils import force_list
from ray.rllib.utils.filter import get_filter
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.space_utils import flatten_space, \
    get_base_struct_from_space, TupleActions

tf = try_import_tf()

logger = logging.getLogger(__name__)


def rollout(policy, env, timestep_limit=None, add_noise=False):
    """Do a rollout.

    If add_noise is True, the rollout will take noisy actions with
    noise drawn from that stream. Otherwise, no action noise will be added.
    """
    max_timestep_limit = 999999
    env_timestep_limit = env.spec.max_episode_steps if (
            hasattr(env, "spec") and hasattr(env.spec, "max_episode_steps")) \
        else max_timestep_limit
    timestep_limit = (env_timestep_limit if timestep_limit is None else min(
        timestep_limit, env_timestep_limit))
    rewards = []
    t = 0
    observation = env.reset()
    for _ in range(timestep_limit or max_timestep_limit):
        env_action = policy.compute_actions(
            observation, add_noise=add_noise)[0]
        observation, reward, done, _ = env.step(env_action)
        rewards.append(reward)
        t += 1
        if done:
            break
    rewards = np.array(rewards, dtype=np.float32)
    return rewards, t


class GenericPolicy:
    def __init__(self, sess, action_space, obs_space, preprocessor,
                 observation_filter, model_options, action_noise_std):
        self.sess = sess
        self.action_space = action_space
        self.flattened_action_space = flatten_space(action_space)
        self.action_space_struct = get_base_struct_from_space(action_space)
        self.action_noise_std = action_noise_std
        self.preprocessor = preprocessor
        self.observation_filter = get_filter(observation_filter,
                                             self.preprocessor.shape)
        self.inputs = tf.placeholder(tf.float32,
                                     [None] + list(self.preprocessor.shape))

        # Policy network.
        dist_class, dist_dim = ModelCatalog.get_action_dist(
            self.action_space, model_options, dist_type="deterministic")
        model = ModelCatalog.get_model({
            "obs": self.inputs
        }, obs_space, action_space, dist_dim, model_options)
        dist = dist_class(model.outputs, model)
        self.sampler = dist.sample()

        self.variables = ray.experimental.tf_utils.TensorFlowVariables(
            model.outputs, self.sess)

        self.num_params = sum(
            np.prod(variable.shape.as_list())
            for _, variable in self.variables.variables.items())
        self.sess.run(tf.global_variables_initializer())

    def compute_actions(self, observation, add_noise=False, update=True):
        observation = self.preprocessor.transform(observation)
        observation = self.observation_filter(observation[None], update=update)
        # `actions` is a list of (component) batches.
        actions = self.sess.run(
            self.sampler, feed_dict={self.inputs: observation})
        if isinstance(actions, TupleActions):
            actions = actions.batches
        flat_actions = force_list(actions)
        if add_noise:
            flat_actions = tree.map_structure(self._add_noise, flat_actions,
                                              self.flattened_action_space)
        # Convert `flat_actions` to a list of lists of action components
        # (list of single actions).
        flat_actions = unbatch_actions(flat_actions)
        env_actions = [
            tree.unflatten_as(self.action_space_struct, f)
            for f in flat_actions
        ]
        return env_actions

    def _add_noise(self, single_action, single_action_space):
        if isinstance(single_action_space, gym.spaces.Box):
            single_action += np.random.randn(*single_action.shape) * \
                self.action_noise_std
        return single_action

    def set_weights(self, x):
        self.variables.set_flat(x)

    def get_weights(self):
        return self.variables.get_flat()

    def get_filter(self):
        return self.observation_filter

    def set_filter(self, observation_filter):
        self.observation_filter = observation_filter

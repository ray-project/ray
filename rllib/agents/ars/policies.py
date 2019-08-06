# Code in this file is copied and adapted from
# https://github.com/openai/evolution-strategies-starter.

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import gym
import numpy as np

import ray
import ray.experimental.tf_utils
from ray.rllib.evaluation.sampler import _unbatch_tuple_actions
from ray.rllib.utils.filter import get_filter
from ray.rllib.models import ModelCatalog
from ray.rllib.utils import try_import_tf

tf = try_import_tf()


def rollout(policy, env, timestep_limit=None, add_noise=False, offset=0):
    """Do a rollout.

    If add_noise is True, the rollout will take noisy actions with
    noise drawn from that stream. Otherwise, no action noise will be added.

    Parameters
    ----------
    policy: tf object
        policy from which to draw actions
    env: GymEnv
        environment from which to draw rewards, done, and next state
    timestep_limit: int, optional
        steps after which to end the rollout
    add_noise: bool, optional
        indicates whether exploratory action noise should be added
    offset: int, optional
        value to subtract from the reward. For example, survival bonus
        from humanoid
    """
    env_timestep_limit = env.spec.max_episode_steps
    timestep_limit = (env_timestep_limit if timestep_limit is None else min(
        timestep_limit, env_timestep_limit))
    rews = []
    t = 0
    observation = env.reset()
    for _ in range(timestep_limit or 999999):
        ac = policy.compute(observation, add_noise=add_noise, update=True)[0]
        observation, rew, done, _ = env.step(ac)
        rew -= np.abs(offset)
        rews.append(rew)
        t += 1
        if done:
            break
    rews = np.array(rews, dtype=np.float32)
    return rews, t


class GenericPolicy(object):
    def __init__(self,
                 sess,
                 action_space,
                 obs_space,
                 preprocessor,
                 observation_filter,
                 model_config,
                 action_noise_std=0.0):
        self.sess = sess
        self.action_space = action_space
        self.action_noise_std = action_noise_std
        self.preprocessor = preprocessor
        self.observation_filter = get_filter(observation_filter,
                                             self.preprocessor.shape)
        self.inputs = tf.placeholder(tf.float32,
                                     [None] + list(self.preprocessor.shape))

        # Policy network.
        dist_class, dist_dim = ModelCatalog.get_action_dist(
            action_space, model_config, dist_type="deterministic")

        model = ModelCatalog.get_model({
            "obs": self.inputs
        }, obs_space, action_space, dist_dim, model_config)
        dist = dist_class(model.outputs, model_config=model_config)
        self.sampler = dist.sample()

        self.variables = ray.experimental.tf_utils.TensorFlowVariables(
            model.outputs, self.sess)

        self.num_params = sum(
            np.prod(variable.shape.as_list())
            for _, variable in self.variables.variables.items())
        self.sess.run(tf.global_variables_initializer())

    def compute(self, observation, add_noise=False, update=True):
        observation = self.preprocessor.transform(observation)
        observation = self.observation_filter(observation[None], update=update)
        action = self.sess.run(
            self.sampler, feed_dict={self.inputs: observation})
        action = _unbatch_tuple_actions(action)
        if add_noise and isinstance(self.action_space, gym.spaces.Box):
            action += np.random.randn(*action.shape) * self.action_noise_std
        return action

    def set_weights(self, x):
        self.variables.set_flat(x)

    def set_filter(self, obs_filter):
        self.observation_filter = obs_filter

    def get_filter(self):
        return self.observation_filter

    def get_weights(self):
        return self.variables.get_flat()

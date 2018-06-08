'''
Policy class for computing action from weights and observation vector. 
Horia Mania --- hmania@berkeley.edu
Aurelia Guy
Benjamin Recht 
'''


from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import gym
import numpy as np
import tensorflow as tf

import ray
from ray.rllib.models import ModelCatalog
from ray.rllib.utils.filter import get_filter


def rollout(policy, env, timestep_limit=None, add_noise=False):
    """Do a rollout.

    If add_noise is True, the rollout will take noisy actions with
    noise drawn from that stream. Otherwise, no action noise will be added.
    """
    env_timestep_limit = env.spec.max_episode_steps
    timestep_limit = (env_timestep_limit if timestep_limit is None
                      else min(timestep_limit, env_timestep_limit))
    rews = []
    t = 0
    observation = env.reset()
    for _ in range(timestep_limit or 999999):
        ac = policy.compute(observation, add_noise=add_noise)[0]
        observation, rew, done, _ = env.step(ac)
        rews.append(rew)
        t += 1
        if done:
            break
    rews = np.array(rews, dtype=np.float32)
    return rews, t


class MLPPolicy(object):
    def __init__(self, registry, sess, action_space, preprocessor,
                 observation_filter):
        self.sess = sess
        self.action_space = action_space
        self.preprocessor = preprocessor
        self.observation_filter = get_filter(
            observation_filter, self.preprocessor.shape)
        self.inputs = tf.placeholder(
            tf.float32, [None] + list(self.preprocessor.shape))

        # Policy network.
        dist_class, dist_dim = ModelCatalog.get_action_dist(
            self.action_space, dist_type="deterministic")
        model = ModelCatalog.get_model(registry, self.inputs, dist_dim,
                                       options={"fcnet_hiddens": [32, 32]})
        dist = dist_class(model.outputs)
        self.sampler = dist.sample()

        self.variables = ray.experimental.TensorFlowVariables(
            model.outputs, self.sess)

        self.num_params = sum([np.prod(variable.shape.as_list())
                               for _, variable
                               in self.variables.variables.items()])
        self.sess.run(tf.global_variables_initializer())

    def compute(self, observation, update=True):
        observation = self.preprocessor.transform(observation)
        observation = self.observation_filter(observation[None], update=update)
        action = self.sess.run(self.sampler,
                               feed_dict={self.inputs: observation})
        return np.reshape(action, -1)

    def set_weights(self, x):
        self.variables.set_flat(x)

    def get_weights(self):
        return self.variables.get_flat()

    def get_weights_plus_stats(self):
        mu, std = self.observation_filter.get_stats()
        aux = np.asarray([self.get_weights(), mu, std])
        return aux


class LinearPolicy(object):
    def __init__(self, registry, sess, action_space, preprocessor,
                 observation_filter):
        self.sess = sess
        self.action_space = action_space
        self.preprocessor = preprocessor
        # self.observation_filter = get_filter(
        #     observation_filter, self.preprocessor.shape)
        # self.inputs = tf.placeholder(
        #     tf.float32, [None] + list(self.preprocessor.shape))
        #
        # # Policy network.
        # dist_class, dist_dim = ModelCatalog.get_action_dist(
        #     self.action_space, dist_type="deterministic")
        # model = ModelCatalog.get_model(registry, self.inputs, dist_dim)
        # dist = dist_class(model.outputs)
        # self.sampler = dist.sample()
        #
        # self.variables = ray.experimental.TensorFlowVariables(
        #     model.outputs, self.sess)
        #
        # self.num_params = sum([np.prod(variable.shape.as_list())
        #                        for _, variable
        #                        in self.variables.variables.items()])
        # self.sess.run(tf.global_variables_initializer())
        self.weights = np.zeros((action_space.shape[0], preprocessor.shape[0]), dtype=np.float64)

    def compute(self, observation, update=True):
        observation = self.preprocessor.transform(observation)
        # observation = self.observation_filter(observation[None], update=update)
        # print('post filter observation is', observation)
        # action = self.sess.run(self.sampler,
        #                        feed_dict={self.inputs: observation})
        action = np.dot(self.weights, observation)
        return np.reshape(action, -1)

    def set_weights(self, x):
        self.weights = np.reshape(x, self.weights.shape)

    def get_weights(self):
        return self.weights.flatten()

# Code in this file is copied and adapted from
# https://github.com/openai/evolution-strategies-starter.

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging

import gym.spaces
import numpy as np
import tensorflow as tf

from ray.rllib.es import tf_util as U
from ray.rllib.models import ModelCatalog

logger = logging.getLogger(__name__)


class GenericPolicy(object):
    def __init__(self, *args, **kwargs):
        self.args, self.kwargs = args, kwargs
        self.scope = self._initialize(*args, **kwargs)
        self.all_variables = tf.get_collection(tf.GraphKeys.GLOBAL_VARIABLES,
                                               self.scope.name)

        self.trainable_variables = tf.get_collection(
            tf.GraphKeys.TRAINABLE_VARIABLES, self.scope.name)
        self.num_params = sum(int(np.prod(v.get_shape().as_list()))
                              for v in self.trainable_variables)
        self._setfromflat = U.SetFromFlat(self.trainable_variables)
        self._getflat = U.GetFlat(self.trainable_variables)

        logger.info('Trainable variables ({} parameters)'
                    .format(self.num_params))
        for v in self.trainable_variables:
            shp = v.get_shape().as_list()
            logger.info('- {} shape:{} size:{}'.format(v.name, shp,
                                                       np.prod(shp)))
        logger.info('All variables')
        for v in self.all_variables:
            shp = v.get_shape().as_list()
            logger.info('- {} shape:{} size:{}'.format(v.name, shp,
                                                       np.prod(shp)))

        placeholders = [tf.placeholder(v.value().dtype,
                                       v.get_shape().as_list())
                        for v in self.all_variables]
        self.set_all_vars = U.function(
            inputs=placeholders,
            outputs=[],
            updates=[tf.group(*[v.assign(p) for v, p
                     in zip(self.all_variables, placeholders)])]
        )

    def _initialize(self, ob_space, ac_space, preprocessor, ac_noise_std):
        self.ac_space = ac_space
        self.ac_noise_std = ac_noise_std
        self.preprocessor = preprocessor

        with tf.variable_scope(type(self).__name__) as scope:
            # TODO(ekl): we should do clipping in a standard RLlib preprocessor
            inputs = tf.placeholder(
                tf.float32, [None] + list(self.preprocessor.shape))

            # Policy network.
            dist_class, dist_dim = ModelCatalog.get_action_dist(
                self.ac_space, dist_type='deterministic')
            model = ModelCatalog.get_model(inputs, dist_dim)
            dist = dist_class(model.outputs)
            self._act = U.function([inputs], dist.sample())
        return scope

    def act(self, ob, random_stream=None):
        a = self._act(ob)
        if not isinstance(self.ac_space, gym.spaces.Discrete) and \
                random_stream is not None and self.ac_noise_std != 0:
            a += random_stream.randn(*a.shape) * self.ac_noise_std
        return a

    def rollout(self, env, preprocessor, render=False, timestep_limit=None,
                save_obs=False, random_stream=None):
        """Do a rollout.

        If random_stream is provided, the rollout will take noisy actions with
        noise drawn from that stream. Otherwise, no action noise will be added.
        """
        env_timestep_limit = env.spec.tags.get("wrapper_config.TimeLimit"
                                               ".max_episode_steps")
        timestep_limit = (env_timestep_limit if timestep_limit is None
                          else min(timestep_limit, env_timestep_limit))
        rews = []
        t = 0
        if save_obs:
            obs = []
        ob = preprocessor.transform(env.reset())
        for _ in range(timestep_limit):
            ac = self.act(ob[None], random_stream=random_stream)[0]
            if save_obs:
                obs.append(ob)
            ob, rew, done, _ = env.step(ac)
            ob = preprocessor.transform(ob)
            rews.append(rew)
            t += 1
            if render:
                env.render()
            if done:
                break
        rews = np.array(rews, dtype=np.float32)
        if save_obs:
            return rews, t, np.array(obs)
        return rews, t

    def set_trainable_flat(self, x):
        self._setfromflat(x)

    def get_trainable_flat(self):
        return self._getflat()

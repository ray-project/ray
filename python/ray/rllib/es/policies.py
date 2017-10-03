# Code in this file is copied and adapted from
# https://github.com/openai/evolution-strategies-starter.

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import pickle

import gym.spaces
import h5py
import numpy as np
import tensorflow as tf

from ray.rllib.es import tf_util as U
from ray.rllib.models import ModelCatalog

logger = logging.getLogger(__name__)


class Policy:
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

    def _initialize(self, *args, **kwargs):
        raise NotImplementedError

    def save(self, filename):
        assert filename.endswith('.h5')
        with h5py.File(filename, 'w') as f:
            for v in self.all_variables:
                f[v.name] = v.eval()
            # TODO: It would be nice to avoid pickle, but it's convenient to
            # pass Python objects to _initialize (like Gym spaces or numpy
            # arrays).
            f.attrs['name'] = type(self).__name__
            f.attrs['args_and_kwargs'] = np.void(pickle.dumps((self.args,
                                                               self.kwargs),
                                                              protocol=-1))

    @classmethod
    def Load(cls, filename, extra_kwargs=None):
        with h5py.File(filename, 'r') as f:
            args, kwargs = pickle.loads(f.attrs['args_and_kwargs'].tostring())
            if extra_kwargs:
                kwargs.update(extra_kwargs)
            policy = cls(*args, **kwargs)
            policy.set_all_vars(*[f[v.name][...]
                                  for v in policy.all_variables])
        return policy

    # === Rollouts/training ===

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

    def act(self, ob, random_stream=None):
        raise NotImplementedError

    def set_trainable_flat(self, x):
        self._setfromflat(x)

    def get_trainable_flat(self):
        return self._getflat()

    @property
    def needs_ob_stat(self):
        raise NotImplementedError

    def set_ob_stat(self, ob_mean, ob_std):
        raise NotImplementedError


def bins(x, dim, num_bins, name):
    scores = U.dense(x, dim * num_bins, name, U.normc_initializer(0.01))
    scores_nab = tf.reshape(scores, [-1, dim, num_bins])
    return tf.argmax(scores_nab, 2)


class GenericPolicy(Policy):
    def _initialize(self, ob_space, ac_space, preprocessor, ac_noise_std):
        self.ac_space = ac_space
        self.ac_noise_std = ac_noise_std
        self.preprocessor_shape = preprocessor.transform_shape(ob_space.shape)

        with tf.variable_scope(type(self).__name__) as scope:
            # Observation normalization.
            ob_mean = tf.get_variable(
                'ob_mean', self.preprocessor_shape, tf.float32,
                tf.constant_initializer(np.nan), trainable=False)
            ob_std = tf.get_variable(
                'ob_std', self.preprocessor_shape, tf.float32,
                tf.constant_initializer(np.nan), trainable=False)
            in_mean = tf.placeholder(tf.float32, self.preprocessor_shape)
            in_std = tf.placeholder(tf.float32, self.preprocessor_shape)
            self._set_ob_mean_std = U.function([in_mean, in_std], [], updates=[
                tf.assign(ob_mean, in_mean),
                tf.assign(ob_std, in_std),
            ])

            inputs = tf.placeholder(
                tf.float32, [None] + list(self.preprocessor_shape))

            # TODO(ekl): we should do clipping in a standard RLlib preprocessor
            clipped_inputs = tf.clip_by_value(
                (inputs - ob_mean) / ob_std, -5.0, 5.0)

            # Policy network.
            dist_class, dist_dim = ModelCatalog.get_action_dist(
                self.ac_space, dist_type='deterministic')
            model = ModelCatalog.get_model(clipped_inputs, dist_dim)
            dist = dist_class(model.outputs)
            self._act = U.function([inputs], dist.sample())
        return scope

    def act(self, ob, random_stream=None):
        a = self._act(ob)
        if not isinstance(self.ac_space, gym.spaces.Discrete) and \
                random_stream is not None and self.ac_noise_std != 0:
            a += random_stream.randn(*a.shape) * self.ac_noise_std
        return a

    @property
    def needs_ob_stat(self):
        return True

    @property
    def needs_ref_batch(self):
        return False

    def set_ob_stat(self, ob_mean, ob_std):
        self._set_ob_mean_std(ob_mean, ob_std)

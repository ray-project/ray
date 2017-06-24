# Code in this file is copied and adapted from
# https://github.com/openai/evolution-strategies-starter.

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import pickle

import h5py
import numpy as np
import tensorflow as tf

import tf_util as U

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

    logger.info('Trainable variables ({} parameters)'.format(self.num_params))
    for v in self.trainable_variables:
      shp = v.get_shape().as_list()
      logger.info('- {} shape:{} size:{}'.format(v.name, shp, np.prod(shp)))
    logger.info('All variables')
    for v in self.all_variables:
      shp = v.get_shape().as_list()
      logger.info('- {} shape:{} size:{}'.format(v.name, shp, np.prod(shp)))

    placeholders = [tf.placeholder(v.value().dtype, v.get_shape().as_list())
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
      # TODO: It would be nice to avoid pickle, but it's convenient to pass
      # Python objects to _initialize (like Gym spaces or numpy arrays).
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
      policy.set_all_vars(*[f[v.name][...] for v in policy.all_variables])
    return policy

  # === Rollouts/training ===

  def rollout(self, env, render=False, timestep_limit=None, save_obs=False,
              random_stream=None):
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
    ob = env.reset()
    for _ in range(timestep_limit):
      ac = self.act(ob[None], random_stream=random_stream)[0]
      if save_obs:
        obs.append(ob)
      ob, rew, done, _ = env.step(ac)
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


class MujocoPolicy(Policy):
  def _initialize(self, ob_space, ac_space, ac_bins, ac_noise_std, nonlin_type,
                  hidden_dims, connection_type):
    self.ac_space = ac_space
    self.ac_bins = ac_bins
    self.ac_noise_std = ac_noise_std
    self.hidden_dims = hidden_dims
    self.connection_type = connection_type

    assert len(ob_space.shape) == len(self.ac_space.shape) == 1
    assert (np.all(np.isfinite(self.ac_space.low)) and
            np.all(np.isfinite(self.ac_space.high))), "Action bounds required"

    self.nonlin = {'tanh': tf.tanh,
                   'relu': tf.nn.relu,
                   'lrelu': U.lrelu,
                   'elu': tf.nn.elu}[nonlin_type]

    with tf.variable_scope(type(self).__name__) as scope:
      # Observation normalization.
      ob_mean = tf.get_variable(
          'ob_mean', ob_space.shape, tf.float32,
          tf.constant_initializer(np.nan), trainable=False)
      ob_std = tf.get_variable(
          'ob_std', ob_space.shape, tf.float32,
          tf.constant_initializer(np.nan), trainable=False)
      in_mean = tf.placeholder(tf.float32, ob_space.shape)
      in_std = tf.placeholder(tf.float32, ob_space.shape)
      self._set_ob_mean_std = U.function([in_mean, in_std], [], updates=[
          tf.assign(ob_mean, in_mean),
          tf.assign(ob_std, in_std),
      ])

      # Policy network.
      o = tf.placeholder(tf.float32, [None] + list(ob_space.shape))
      a = self._make_net(tf.clip_by_value((o - ob_mean) / ob_std, -5.0, 5.0))
      self._act = U.function([o], a)
    return scope

  def _make_net(self, o):
    # Process observation.
    if self.connection_type == 'ff':
      x = o
      for ilayer, hd in enumerate(self.hidden_dims):
        x = self.nonlin(U.dense(x, hd, 'l{}'.format(ilayer),
                                U.normc_initializer(1.0)))
    else:
      raise NotImplementedError(self.connection_type)

    # Map to action.
    adim = self.ac_space.shape[0]
    ahigh = self.ac_space.high
    alow = self.ac_space.low
    assert isinstance(self.ac_bins, str)
    ac_bin_mode, ac_bin_arg = self.ac_bins.split(':')

    if ac_bin_mode == 'uniform':
      # Uniformly spaced bins, from ac_space.low to ac_space.high.
      num_ac_bins = int(ac_bin_arg)
      aidx_na = bins(x, adim, num_ac_bins, 'out')
      ac_range_1a = (ahigh - alow)[None, :]
      a = (1. / (num_ac_bins - 1.) * tf.to_float(aidx_na) * ac_range_1a +
           alow[None, :])

    elif ac_bin_mode == 'custom':
      # Custom bins specified as a list of values from -1 to 1.
      # The bins are rescaled to ac_space.low to ac_space.high.
      acvals_k = np.array(list(map(float, ac_bin_arg.split(','))),
                          dtype=np.float32)
      logger.info('Custom action values: ' + ' '.join('{:.3f}'.format(x)
                                                      for x in acvals_k))
      assert acvals_k.ndim == 1 and acvals_k[0] == -1 and acvals_k[-1] == 1
      acvals_ak = ((ahigh - alow)[:, None] / (acvals_k[-1] - acvals_k[0]) *
                   (acvals_k - acvals_k[0])[None, :] + alow[:, None])

      aidx_na = bins(x, adim, len(acvals_k), 'out')  # Values in [0, k-1].
      a = tf.gather_nd(
          acvals_ak,
          tf.concat([
              tf.tile(np.arange(adim)[None, :, None],
                      [tf.shape(aidx_na)[0], 1, 1]),
              2,
              tf.expand_dims(aidx_na, -1)
          ])  # (n, a, 2)
      )  # (n, a)
    elif ac_bin_mode == 'continuous':
      a = U.dense(x, adim, 'out', U.normc_initializer(0.01))
    else:
      raise NotImplementedError(ac_bin_mode)

    return a

  def act(self, ob, random_stream=None):
    a = self._act(ob)
    if random_stream is not None and self.ac_noise_std != 0:
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

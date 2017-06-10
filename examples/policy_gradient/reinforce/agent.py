from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from collections import namedtuple

import gym.spaces
import tensorflow as tf
import os

from tensorflow.python.ops.data_flow_ops import StagingArea
from tensorflow.contrib import nccl

import ray

from reinforce.distributions import Categorical, DiagGaussian
from reinforce.env import BatchedEnv
from reinforce.policy import ProximalPolicyLoss
from reinforce.filter import MeanStdFilter
from reinforce.rollout import rollouts, add_advantage_values
from reinforce.utils import make_divisible_by


Tower = namedtuple(
  'Tower',
  ['init_op', 'optimizer', 'grads', 'loss', 'mean_kl', 'mean_entropy'])


def average_gradients(tower_grads):
  """Calculate the average gradient for each shared variable across all towers.
  Note that this function provides a synchronization point across all towers.
  Args:
    tower_grads: List of lists of (gradient, variable) tuples. The outer list
      is over individual gradients. The inner list is over the gradient
      calculation for each tower.
  Returns:
     List of pairs of (gradient, variable) where the gradient has been averaged
     across all towers.
  """

  average_grads = []
  for grad_and_vars in zip(*tower_grads):

    # Note that each grad_and_vars looks like the following:
    #   ((grad0_gpu0, var0_gpu0), ... , (grad0_gpuN, var0_gpuN))
    grads = []
    for g, _ in grad_and_vars:
      if g is not None:
        # Add 0 dimension to the gradients to represent the tower.
        expanded_g = tf.expand_dims(g, 0)

        # Append on a 'tower' dimension which we will average over below.
        grads.append(expanded_g)

    # Average over the 'tower' dimension.
    grad = tf.concat(axis=0, values=grads)
    grad = tf.reduce_mean(grad, 0)
    average_grads.append(grad)

  # Replace all the tower gradients with the average by variable
  final_tower_grads = []
  for tower_grads in tower_grads:
    tower_with_avg_grads = []
    for avg_g, (_, v) in zip(average_grads, tower_grads):
      tower_with_avg_grads.append((avg_g, v))
    final_tower_grads.append(tower_with_avg_grads)

  return final_tower_grads


class Agent(object):
  """
  Implements the graph for both training and evaluation.
  """
  def __init__(self, name, batchsize, preprocessor, config, is_remote):
    with tf.device("/cpu:0"):
      self.do_init(name, batchsize, preprocessor, config, is_remote)

  def do_init(self, name, batchsize, preprocessor, config, is_remote):
    has_gpu = False
    if is_remote:
      os.environ["CUDA_VISIBLE_DEVICES"] = ""
      devices = ["/cpu:0"]
    else:
      devices = config["devices"]
      for device in devices:
        if 'gpu' in device:
          has_gpu = True
    self.devices = devices
    self.config = config
    self.env = BatchedEnv(name, batchsize, preprocessor=preprocessor)
    if preprocessor.shape is None:
      preprocessor.shape = self.env.observation_space.shape
    if is_remote:
      config_proto = tf.ConfigProto()
    else:
      config_proto = tf.ConfigProto(**config["tf_session_args"])
    self.preprocessor = preprocessor
    self.sess = tf.Session(config=config_proto)

    # Defines the training inputs.
    self.kl_coeff = tf.placeholder(name="newkl", shape=(), dtype=tf.float32)
    self.observations = tf.placeholder(tf.float32,
                                       shape=(None,) + preprocessor.shape)
    self.advantages = tf.placeholder(tf.float32, shape=(None,))

    action_space = self.env.action_space
    if isinstance(action_space, gym.spaces.Box):
      # The first half of the dimensions are the means, the second half are the
      # standard deviations.
      self.action_dim = action_space.shape[0]
      self.logit_dim = 2 * self.action_dim
      self.actions = tf.placeholder(tf.float32,
                                    shape=(None, action_space.shape[0]))
      self.distribution_class = DiagGaussian
    elif isinstance(action_space, gym.spaces.Discrete):
      self.action_dim = action_space.n
      self.logit_dim = self.action_dim
      self.actions = tf.placeholder(tf.int64, shape=(None,))
      self.distribution_class = Categorical
    else:
      raise NotImplemented("action space" + str(type(action_space)) +
                           "currently not supported")
    self.prev_logits = tf.placeholder(tf.float32, shape=(None, self.logit_dim))

    data_splits = zip(
        tf.split(self.observations, len(devices)),
        tf.split(self.advantages, len(devices)),
        tf.split(self.actions, len(devices)),
        tf.split(self.prev_logits, len(devices)))

    # Parallel SGD ops
    self.towers = []
    self.batch_index = tf.placeholder(tf.int32)
    self.batch_size = tf.placeholder(tf.int32)
    i = 0
    for device, (obs, adv, acts, plog) in zip(devices, data_splits):
      with tf.device(device):
        self.towers.append(
          self.setup_device(i, obs, adv, acts, plog, reuse_vars=i > 0))
      i += 1

    self.train_op = self.towers[0].optimizer.apply_gradients(
      average_gradients([t.grad for t in self.towers]))

    # Evaluation ops
    with tf.name_scope("test_outputs"):
      self.mean_loss = tf.reduce_mean(
          tf.stack(values=[t.mean_loss for t in self.towers]), 0)
      self.mean_kl = tf.reduce_mean(
          tf.stack(values=[t.mean_kl for t in self.towers]), 0)
      self.mean_entropy = tf.reduce_mean(
          tf.stack(values=[t.mean_entropy for t in self.towers]), 0)

    # References to the model weights
    self.variables = ray.experimental.TensorFlowVariables(
        self.towers[0].loss,  # all towers have equivalent vars
        self.sess)
    self.observation_filter = MeanStdFilter(preprocessor.shape, clip=None)
    self.reward_filter = MeanStdFilter((), clip=5.0)
    self.sess.run(tf.global_variables_initializer())

  def setup_device(
      self, i, observations, advantages, actions, prev_logits, reuse_vars):

    with tf.variable_scope("shared_tower_optimizer"):
      if reuse_vars:
        tf.get_variable_scope().reuse_variables()
      optimizer = tf.train.AdamOptimizer(self.config["sgd_stepsize"])

    with tf.variable_scope("tower_" + str(i)):
      all_obs = tf.Variable(observations, trainable=False, validate_shape=False, collections=[])
      all_adv = tf.Variable(advantages, trainable=False, validate_shape=False, collections=[])
      all_acts = tf.Variable(actions, trainable=False, validate_shape=False, collections=[])
      all_plog = tf.Variable(prev_logits, trainable=False, validate_shape=False, collections=[])
      obs_slice = tf.slice(
        all_obs,
        [self.batch_index] + [0] * len(self.preprocessor.shape),
        [self.batch_size] + [-1] * len(self.preprocessor.shape))
      adv_slice = tf.slice(all_adv, [self.batch_index], [self.batch_size])
      acts_slice = tf.slice(all_acts, [self.batch_index], [self.batch_size])
      plog_slice = tf.slice(
          all_plog, [self.batch_index, 0], [self.batch_size, -1])

      ppo = ProximalPolicyLoss(
          self.env.observation_space, self.env.action_space,
          obs_slice, adv_slice, acts_slice, plog_slice, self.logit_dim,
          self.kl_coeff, self.distribution_class, self.config, self.sess)
      grads = self.optimizer.compute_gradients(
          ppo.loss, colocate_gradients_with_ops=True)

    return Tower(
      tf.group(
        [tower_obs.initializer,
         tower_adv.initializer,
         tower_acts.initializer,
         tower_plog.initializer]),
      optimizer,
      grads,
      ppo.loss,
      ppo.mean_kl,
      ppo.mean_entropy)

  def load_data(self, trajectories):
    """
    Bulk loads the specified trajectories into device memory. This data can
    be accessed in batches during sgd training.
    """

    print("Loading rollouts data into device memory")
    truncated_obs = make_divisible_by(
      trajectories["observations"], len(self.devices)),
    self.sess.run(
      [t.init_op for t in self.towers],
      feed_dict={
        self.observations: truncated_obs,
        self.advantages: make_divisible_by(
            trajectories["advantages"], len(self.devices)),
        self.actions: make_divisible_by(
            trajectories["actions"].squeeze(), len(self.devices)),
        self.prev_logits: make_divisible_by(
            trajectories["logprobs"], len(self.devices)),
      })
    print("Done loading data")
    self.tuples_per_device = len(truncated_obs) / len(self.devices)

  def run_sgd_minibatch(self, batch_index, batch_size, kl_coeff):
    """
    Runs a SGD step over the batch with index batch_index as created by
    load_rollouts_data(), updating local weights.
    """

    self.sess.run(
        [self.train_op],
        feed_dict={
            self.batch_index: batch_index,
            self.batch_size: batch_size,
            self.kl_coeff: kl_coeff})

  def get_test_stats(self, kl_coeff):
    """
    Returns (mean_loss, mean_kl, mean_entropy) of the current model evaluated
    over all the currently loaded rollouts data.
    """

    return self.sess.run(
        [self.mean_loss, self.mean_kl, self.mean_entropy],
        feed_dict={
            self.batch_index: 0,
            self.batch_size: self.tuples_per_device,
            self.kl_coeff: kl_coeff})

  def get_weights(self):
    return self.variables.get_weights()

  def load_weights(self, weights):
    self.variables.set_weights(weights)

  def compute_trajectory(self, gamma, lam, horizon):
    trajectory = rollouts(
        self.ppo_towers[0],  # all the towers have the same weights
        self.env, horizon, self.observation_filter, self.reward_filter)
    add_advantage_values(trajectory, gamma, lam, self.reward_filter)
    return trajectory


RemoteAgent = ray.remote(Agent)

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from collections import namedtuple

import gym.spaces
import tensorflow as tf
import os

from tensorflow.python.client import timeline

import ray

from reinforce.distributions import Categorical, DiagGaussian
from reinforce.env import BatchedEnv
from reinforce.policy import ProximalPolicyLoss
from reinforce.filter import MeanStdFilter
from reinforce.rollout import rollouts, add_advantage_values
from reinforce.utils import make_divisible_by


Tower = namedtuple('Tower', ['init_op', 'grads', 'policy'])


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

    # Keep in mind that the Variables are redundant because they are shared
    # across towers. So .. we will just return the first tower's pointer to
    # the Variable.
    v = grad_and_vars[0][1]
    grad_and_var = (grad, v)
    average_grads.append(grad_and_var)

  return average_grads


class Agent(object):
  """
  Implements the graph for both training and evaluation.
  """
  def __init__(self, name, batchsize, preprocessor, config, is_remote):
    with tf.device("/cpu:0"):
      self.do_init(name, batchsize, preprocessor, config, is_remote)

  def do_init(self, name, batchsize, preprocessor, config, is_remote):
    if is_remote:
      os.environ["CUDA_VISIBLE_DEVICES"] = ""
      devices = ["/cpu:0"]
    else:
      devices = config["devices"]
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
    assert config["sgd_batchsize"] % len(devices) == 0, \
        "Batch size must be evenly divisible by devices"
    if is_remote:
      self.batch_size = 1
      self.per_device_batch_size = 1
    else:
      self.batch_size = config["sgd_batchsize"]
      self.per_device_batch_size = int(self.batch_size / len(devices))
    self.optimizer = tf.train.AdamOptimizer(self.config["sgd_stepsize"])
    self.setup_global_policy(
        self.observations, self.advantages, self.actions, self.prev_logits)
    for device, (obs, adv, acts, plog) in zip(devices, data_splits):
      self.towers.append(self.setup_device(device, obs, adv, acts, plog))

    avg = average_gradients([t.grads for t in self.towers])
    self.train_op = self.optimizer.apply_gradients(avg)

    # Metric ops
    with tf.name_scope("test_outputs"):
      self.mean_loss = tf.reduce_mean(
          tf.stack(values=[t.policy.loss for t in self.towers]), 0)
      self.mean_kl = tf.reduce_mean(
          tf.stack(values=[t.policy.mean_kl for t in self.towers]), 0)
      self.mean_entropy = tf.reduce_mean(
          tf.stack(values=[t.policy.mean_entropy for t in self.towers]), 0)

    # References to the model weights
    self.variables = ray.experimental.TensorFlowVariables(
        self.global_policy.loss,
        self.sess)
    self.observation_filter = MeanStdFilter(preprocessor.shape, clip=None)
    self.reward_filter = MeanStdFilter((), clip=5.0)
    self.sess.run(tf.global_variables_initializer())

  def setup_global_policy(self, observations, advantages, actions, prev_log):
    with tf.variable_scope("tower"):
      self.global_policy = ProximalPolicyLoss(
          self.env.observation_space, self.env.action_space,
          observations, advantages, actions, prev_log, self.logit_dim,
          self.kl_coeff, self.distribution_class, self.config, self.sess)

  def setup_device(self, device, observations, advantages, actions, prev_log):
    with tf.device(device):
      with tf.variable_scope("tower", reuse=True):
        all_obs = tf.Variable(
            observations, trainable=False, validate_shape=False,
            collections=[])
        all_adv = tf.Variable(
            advantages, trainable=False, validate_shape=False, collections=[])
        all_acts = tf.Variable(
            actions, trainable=False, validate_shape=False, collections=[])
        all_plog = tf.Variable(
            prev_log, trainable=False, validate_shape=False, collections=[])
        obs_slice = tf.slice(
            all_obs,
            [self.batch_index] + [0] * len(self.preprocessor.shape),
            [self.per_device_batch_size] + [-1] * len(self.preprocessor.shape))
        obs_slice.set_shape(observations.shape)
        adv_slice = tf.slice(
            all_adv, [self.batch_index], [self.per_device_batch_size])
        acts_slice = tf.slice(
            all_acts, [self.batch_index], [self.per_device_batch_size])
        plog_slice = tf.slice(
            all_plog, [self.batch_index, 0], [self.per_device_batch_size, -1])
        policy = ProximalPolicyLoss(
            self.env.observation_space, self.env.action_space,
            obs_slice, adv_slice, acts_slice, plog_slice, self.logit_dim,
            self.kl_coeff, self.distribution_class, self.config, self.sess)
        grads = self.optimizer.compute_gradients(
            policy.loss, colocate_gradients_with_ops=True)

      return Tower(
          tf.group(
              *[all_obs.initializer,
                all_adv.initializer,
                all_acts.initializer,
                all_plog.initializer]),
          grads,
          policy)

  def load_data(self, trajectories, full_trace):
    """
    Bulk loads the specified trajectories into device memory. This data can
    be accessed in batches during sgd training.
    """

    truncated_obs = make_divisible_by(
        trajectories["observations"], self.batch_size)
    if full_trace:
      run_options = tf.RunOptions(trace_level=tf.RunOptions.FULL_TRACE)
    else:
      run_options = tf.RunOptions(trace_level=tf.RunOptions.NO_TRACE)
    run_metadata = tf.RunMetadata()
    self.sess.run(
        [t.init_op for t in self.towers],
        feed_dict={
            self.observations: truncated_obs,
            self.advantages: make_divisible_by(
                trajectories["advantages"], self.batch_size),
            self.actions: make_divisible_by(
                trajectories["actions"].squeeze(), self.batch_size),
            self.prev_logits: make_divisible_by(
                trajectories["logprobs"], self.batch_size),
        },
        options=run_options,
        run_metadata=run_metadata)
    if full_trace:
      trace = timeline.Timeline(step_stats=run_metadata.step_stats)
      trace_file = open('/tmp/ray/timeline-load.json', 'w')
      trace_file.write(trace.generate_chrome_trace_format())
    self.tuples_per_device = len(truncated_obs) / len(self.devices)
    assert self.tuples_per_device % self.per_device_batch_size == 0

  def run_sgd_minibatch(self, batch_index, kl_coeff, full_trace, file_writer):
    """
    Runs a SGD step over the batch with index batch_index as created by
    load_rollouts_data(), updating local weights.

    Returns (mean_loss, mean_kl, mean_entropy) evaluated over the batch.
    """

    if full_trace:
      run_options = tf.RunOptions(trace_level=tf.RunOptions.FULL_TRACE)
    else:
      run_options = tf.RunOptions(trace_level=tf.RunOptions.NO_TRACE)
    run_metadata = tf.RunMetadata()

    _, loss, kl, entropy = self.sess.run(
        [self.train_op, self.mean_loss, self.mean_kl, self.mean_entropy],
        feed_dict={
            self.batch_index: batch_index,
            self.kl_coeff: kl_coeff},
        options=run_options,
        run_metadata=run_metadata)

    if full_trace:
      trace = timeline.Timeline(step_stats=run_metadata.step_stats)
      trace_file = open('/tmp/ray/timeline-sgd.json', 'w')
      trace_file.write(trace.generate_chrome_trace_format())
      file_writer.add_run_metadata(
          run_metadata, "sgd_train_{}".format(batch_index))

    return loss, kl, entropy

  def get_weights(self):
    return self.variables.get_weights()

  def load_weights(self, weights):
    self.variables.set_weights(weights)

  def compute_trajectory(self, gamma, lam, horizon):
    trajectory = rollouts(
        self.global_policy,
        self.env, horizon, self.observation_filter, self.reward_filter)
    add_advantage_values(trajectory, gamma, lam, self.reward_filter)
    return trajectory


RemoteAgent = ray.remote(Agent)

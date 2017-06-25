from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from collections import namedtuple

import gym.spaces
import tensorflow as tf
import os

from tensorflow.python.client import timeline
from tensorflow.python import debug as tf_debug

import ray

from ray.rllib.policy_gradient.distributions import Categorical, DiagGaussian
from ray.rllib.policy_gradient.env import BatchedEnv
from ray.rllib.policy_gradient.loss import ProximalPolicyLoss
from ray.rllib.policy_gradient.filter import MeanStdFilter
from ray.rllib.policy_gradient.rollout import rollouts, add_advantage_values
from ray.rllib.policy_gradient.utils import (
    make_divisible_by, average_gradients)

# TODO(pcm): Make sure that both observation_filter and reward_filter
# are correctly handled, i.e. (a) the values are accumulated accross
# workers (if necessary), (b) they are passed between all the methods
# correctly and no default arguments are used, and (c) they are saved
# as part of the checkpoint so training can resume properly.

# Each tower is a copy of the policy graph pinned to a specific device.
Tower = namedtuple("Tower", ["init_op", "grads", "policy"])


class Agent(object):
  """
  Agent class that holds the simulator environment and the policy.

  Initializes the tensorflow graphs for both training and evaluation.
  One common policy graph is initialized on '/cpu:0' and holds all the shared
  network weights. When run as a remote agent, only this graph is used.

  When the agent is initialized locally with multiple GPU devices, copies of
  the policy graph are also placed on each GPU. These per-GPU graphs share the
  common policy network weights but take device-local input tensors.

  The idea here is that training data can be bulk-loaded onto these
  device-local variables. Synchronous SGD can then be run in parallel over
  this GPU-local data.
  """

  def __init__(self, name, batchsize, preprocessor, config, is_remote):
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
    if config["use_tf_debugger"] and not is_remote:
      self.sess = tf_debug.LocalCLIDebugWrapperSession(self.sess)
      self.sess.add_tensor_filter("has_inf_or_nan", tf_debug.has_inf_or_nan)

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
      self.action_shape = (self.action_dim,)
      self.logit_dim = 2 * self.action_dim
      self.actions = tf.placeholder(tf.float32, shape=(None, self.action_dim))
      self.distribution_class = DiagGaussian
    elif isinstance(action_space, gym.spaces.Discrete):
      self.action_dim = action_space.n
      self.action_shape = ()
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
    self.setup_common_policy(
        self.observations, self.advantages, self.actions, self.prev_logits)
    for device, (obs, adv, acts, plog) in zip(devices, data_splits):
      self.towers.append(
          self.setup_per_device_policy(device, obs, adv, acts, plog))

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
        self.common_policy.loss,
        self.sess)
    self.observation_filter = MeanStdFilter(preprocessor.shape, clip=None)
    self.reward_filter = MeanStdFilter((), clip=5.0)
    self.sess.run(tf.global_variables_initializer())

  def setup_common_policy(self, observations, advantages, actions, prev_log):
    with tf.variable_scope("tower"):
      self.common_policy = ProximalPolicyLoss(
          self.env.observation_space, self.env.action_space,
          observations, advantages, actions, prev_log, self.logit_dim,
          self.kl_coeff, self.distribution_class, self.config, self.sess)

  def setup_per_device_policy(
          self, device, observations, advantages, actions, prev_log):
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
            all_acts,
            [self.batch_index] + [0] * len(self.action_shape),
            [self.per_device_batch_size] + [-1] * len(self.action_shape))
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
    Bulk loads the specified trajectories into device memory.

    The data is split equally across all the devices.

    Returns:
      The number of tuples loaded per device.
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
      trace_file = open("/tmp/ray/timeline-load.json", "w")
      trace_file.write(trace.generate_chrome_trace_format())

    tuples_per_device = len(truncated_obs) / len(self.devices)
    assert tuples_per_device % self.per_device_batch_size == 0
    return tuples_per_device

  def run_sgd_minibatch(self, batch_index, kl_coeff, full_trace, file_writer):
    """
    Run a single step of SGD.

    Runs a SGD step over the batch with index batch_index as created by
    load_rollouts_data(), updating local weights.

    Returns:
      (mean_loss, mean_kl, mean_entropy) evaluated over the batch.
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
      trace_file = open("/tmp/ray/timeline-sgd.json", "w")
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
        self.common_policy,
        self.env, horizon, self.observation_filter, self.reward_filter)
    add_advantage_values(trajectory, gamma, lam, self.reward_filter)
    return trajectory


RemoteAgent = ray.remote(Agent)

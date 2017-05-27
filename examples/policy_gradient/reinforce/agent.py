from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

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


def average_gradients_gpu(tower_grads):
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

  averages = []
  # For each variable
  for grad_and_vars in zip(*tower_grads):
    # Note that each grad_and_vars looks like the following:
    #   ((grad0_gpu0, var0_gpu0), ... , (grad0_gpuN, var0_gpuN))
    grads = [g for (g, _) in grad_and_vars if g is not None]
    print("Grads", grads)
    avg_grads = nccl.all_sum(grads)
    print("Avg grads", avg_grads)
    # Replace grad_gpuN with the average grad across all gpus, e.g.
    #   ((avg_grad_0, var0_gpu0), ... , (avg_grad_0, var0_gpuN))
    # TODO(ekl) divide by num towers
    average_by_grad = [
        (avg_g, v) for (avg_g, (_, v)) in zip(avg_grads, grad_and_vars)]
    averages.append(average_by_grad)

  # Transpose the lists to be by tower
  return zip(*averages)


def average_gradients_cpu(tower_grads):
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
  def __init__(self, name, batchsize, preprocessor, config, use_gpu):
    with tf.device("/cpu:0"):
      self.do_init(name, batchsize, preprocessor, config, use_gpu)

  def do_init(self, name, batchsize, preprocessor, config, use_gpu):
    if not use_gpu:
      os.environ["CUDA_VISIBLE_DEVICES"] = ""
      devices = ["/cpu:0"]
    else:
      devices = config["devices"]
    self.env = BatchedEnv(name, batchsize, preprocessor=preprocessor)
    if preprocessor.shape is None:
      preprocessor.shape = self.env.observation_space.shape
    if use_gpu:
      config_proto = tf.ConfigProto(**config["tf_session_args"])
    else:
      config_proto = tf.ConfigProto()
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
      distribution_class = DiagGaussian
    elif isinstance(action_space, gym.spaces.Discrete):
      self.action_dim = action_space.n
      self.logit_dim = self.action_dim
      self.actions = tf.placeholder(tf.int64, shape=(None,))
      distribution_class = Categorical
    else:
      raise NotImplemented("action space" + str(type(action_space)) +
                           "currently not supported")
    self.prev_logits = tf.placeholder(tf.float32, shape=(None, self.logit_dim))

    # Defines operations for staging input training data.
    self.num_splits = len(devices)
    stage = StagingArea(
        [self.observations.dtype, self.advantages.dtype,
         self.actions.dtype, self.prev_logits.dtype],
        [self.observations.shape, self.advantages.shape,
         self.actions.shape, self.prev_logits.shape])
    tower_stage_ops = []
    data_tuples = zip(
        tf.split(self.observations, self.num_splits),
        tf.split(self.advantages, self.num_splits),
        tf.split(self.actions, self.num_splits),
        tf.split(self.prev_logits, self.num_splits))
    for item in data_tuples:
        p_op = stage.put(item)
        tower_stage_ops.append(p_op)
    self.stage_trajectory_data_op = tf.group(*tower_stage_ops)

    # Defines the model replicas (i.e. "towers"), one per device.
    self.ppo_towers = []
    optimizers = []
    grads = []
    for i, device in enumerate(devices):
      with tf.device(device):
        with tf.variable_scope("tower_" + str(i)):
          obs, adv, acts, plgs = stage.get()
          ppo = ProximalPolicyLoss(
              self.env.observation_space, self.env.action_space,
              obs, adv, acts, plgs, self.logit_dim, self.kl_coeff,
              distribution_class, config, self.sess)
          self.ppo_towers.append(ppo)
          optimizer = tf.train.AdamOptimizer(config["sgd_stepsize"])
          grads.append(optimizer.compute_gradients(ppo.loss))
          optimizers.append(optimizer)

    if use_gpu:
      average_grads = average_gradients_gpu(grads)
    else:
      average_grads = average_gradients_cpu(grads)
    tower_train_ops = []
    for i, (device, opt, avg_grad) in
          enumerate(zip(devices, optimizers, average_grads)):
      with tf.device(device):
        tower_train_ops.append(opt.apply_gradients(avg_grad))

    # The final training op which executes in parallel over the model towers.
    self.train_op = tf.group(*tower_train_ops)

    # Metric ops
    with tf.name_scope("test_outputs"):
      self.mean_loss = tf.reduce_mean(
          tf.stack(values=[p.loss for p in self.ppo_towers]), 0)
      self.mean_kl = tf.reduce_mean(
          tf.stack(values=[p.mean_kl for p in self.ppo_towers]), 0)
      self.mean_entropy = tf.reduce_mean(
          tf.stack(values=[p.mean_entropy for p in self.ppo_towers]), 0)

    # References to the model weights
    self.variables = ray.experimental.TensorFlowVariables(
        self.ppo_towers[0].loss,  # all towers have equivalent vars
        self.sess)
    self.observation_filter = MeanStdFilter(preprocessor.shape, clip=None)
    self.reward_filter = MeanStdFilter((), clip=5.0)
    self.sess.run(tf.global_variables_initializer())

  def make_feed_dict(self, batch, kl_coeff):
    if batch is None:
      inputs = {
          self.kl_coeff: kl_coeff
      }
    else:
      inputs = {
          self.observations: make_divisible_by(
              batch["observations"], self.num_splits),
          self.advantages: make_divisible_by(
              batch["advantages"], self.num_splits),
          self.actions: make_divisible_by(
              batch["actions"].squeeze(), self.num_splits),
          self.prev_logits: make_divisible_by(
              batch["logprobs"], self.num_splits),
          self.kl_coeff: kl_coeff,
      }
    return inputs

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

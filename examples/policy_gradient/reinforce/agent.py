from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import gym.spaces
import tensorflow as tf
import os

import ray

from reinforce.distributions import Categorical, DiagGaussian
from reinforce.env import BatchedEnv
from reinforce.policy import ProximalPolicyLoss
from reinforce.filter import MeanStdFilter
from reinforce.rollout import rollouts, add_advantage_values


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
  def __init__(self, name, batchsize, preprocessor, config, use_gpu):
    if not use_gpu:
      os.environ["CUDA_VISIBLE_DEVICES"] = ""
      devices = ["/cpu:0"]
    else:
      devices = config["devices"]
    self.env = BatchedEnv(name, batchsize, preprocessor=preprocessor)
    if preprocessor.shape is None:
      preprocessor.shape = self.env.observation_space.shape
    config_proto = tf.ConfigProto(**config["tf_session_args"])
    self.sess = tf.Session(config=config_proto)
    with tf.name_scope("policy_gradient"):
      with tf.name_scope("compute_gradient"):
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

        # Tower parallelization
        num_devices = len(devices)
        observations_parts = tf.split(self.observations, num_devices)
        advantages_parts = tf.split(self.advantages, num_devices)
        actions_parts = tf.split(self.actions, num_devices)
        prev_logits_parts = tf.split(self.prev_logits, num_devices)

        losses = []
        for i, device in enumerate(devices):
          with tf.name_scope("split_" + str(i)):
            with tf.device(device):
              losses.append(ProximalPolicyLoss(
                  self.env.observation_space, self.env.action_space, preprocessor,
                  observations_parts[i], advantages_parts[i], actions_parts[i],
                  prev_logits_parts[i], self.logit_dim, self.kl_coeff,
                  distribution_class, config, self.sess, report_metrics=i == 0))
        # The policy loss used for rollouts. TODO(ekl) this is quite ugly
        self.ppo = losses[0]
      with tf.name_scope("adam_optimizer"):
        self.optimizer = tf.train.AdamOptimizer(config["sgd_stepsize"])
        grads = []
        for i, device in enumerate(devices):
          with tf.name_scope("split_" + str(i)):
            with tf.device(device):
              grads.append(self.optimizer.compute_gradients(losses[i].loss))
        average_grad = average_gradients(grads)
        self.train_op = self.optimizer.apply_gradients(average_grad)
      self.variables = ray.experimental.TensorFlowVariables(self.ppo.loss,
                                                            self.sess)
      self.observation_filter = MeanStdFilter(preprocessor.shape, clip=None)
      self.reward_filter = MeanStdFilter((), clip=5.0)
    self.summaries = tf.summary.merge_all()
    self.sess.run(tf.global_variables_initializer())

  def get_weights(self):
    return self.variables.get_weights()

  def load_weights(self, weights):
    self.variables.set_weights(weights)

  def compute_trajectory(self, gamma, lam, horizon):
    trajectory = rollouts(self.ppo, self.env, horizon, self.observation_filter,
                          self.reward_filter)
    add_advantage_values(trajectory, gamma, lam, self.reward_filter)
    return trajectory


RemoteAgent = ray.remote(Agent)

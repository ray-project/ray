import tensorflow as tf
import os

import ray

from reinforce.env import BatchedEnv
from reinforce.policy import ProximalPolicyLoss
from reinforce.filter import MeanStdFilter
from reinforce.rollout import rollouts, add_advantage_values

class Agent(object):

  def __init__(self, name, batchsize, config, use_gpu):
    if not use_gpu:
      os.environ["CUDA_VISIBLE_DEVICES"] = ""
    self.env = BatchedEnv(name, batchsize, preprocessor=None)
    self.sess = tf.Session()
    self.ppo = ProximalPolicyLoss(self.env.observation_space, self.env.action_space, config, self.sess)
    self.optimizer = tf.train.AdamOptimizer(config["sgd_stepsize"])
    self.train_op = self.optimizer.minimize(self.ppo.loss)
    self.variables = ray.experimental.TensorFlowVariables(self.ppo.loss, self.sess)
    self.observation_filter = MeanStdFilter(self.env.observation_space.shape, clip=None)
    self.reward_filter = MeanStdFilter((), clip=5.0)
    self.sess.run(tf.global_variables_initializer())

  def get_weights(self):
    return self.variables.get_weights()

  def load_weights(self, weights):
    self.variables.set_weights(weights)

  def compute_trajectory(self, gamma, lam, horizon):
    trajectory = rollouts(self.ppo, self.env, horizon, self.observation_filter, self.reward_filter)
    add_advantage_values(trajectory, gamma, lam, self.reward_filter)
    return trajectory

RemoteAgent = ray.actor(Agent)

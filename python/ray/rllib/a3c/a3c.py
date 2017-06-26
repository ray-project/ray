from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import tensorflow as tf
import six.moves.queue as queue
import os

import ray
from ray.rllib.a3c.LSTM import LSTMPolicy
from ray.rllib.a3c.runner import RunnerThread, process_rollout
from ray.rllib.a3c.envs import create_env
from ray.rllib.common import Algorithm, TrainingResult


DEFAULT_CONFIG = {
    "num_workers": 4,
    "num_rollouts_per_iteration": 100,
}


@ray.remote
class Runner(object):
  """Actor object to start running simulation on workers.

  The gradient computation is also executed from this object.
  """
  def __init__(self, env_name, actor_id, logdir, start=True):
    env = create_env(env_name)
    self.id = actor_id
    num_actions = env.action_space.n
    self.policy = LSTMPolicy(env.observation_space.shape, num_actions,
                             actor_id)
    self.runner = RunnerThread(env, self.policy, 20)
    self.env = env
    self.logdir = logdir
    if start:
      self.start()

  def pull_batch_from_queue(self):
    """Take a rollout from the queue of the thread runner."""
    rollout = self.runner.queue.get(timeout=600.0)
    while not rollout.terminal:
      try:
        rollout.extend(self.runner.queue.get_nowait())
      except queue.Empty:
        break
    return rollout

  def start(self):
    summary_writer = tf.summary.FileWriter(
        os.path.join(self.logdir, "agent_%d" % self.id))
    self.summary_writer = summary_writer
    self.runner.start_runner(self.policy.sess, summary_writer)

  def compute_gradient(self, params):
    self.policy.set_weights(params)
    rollout = self.pull_batch_from_queue()
    batch = process_rollout(rollout, gamma=0.99, lambda_=1.0)
    gradient = self.policy.get_gradients(batch)
    info = {"id": self.id,
            "size": len(batch.a),
            "total_reward": batch.total_reward}
    return gradient, info


class AsynchronousAdvantageActorCritic(Algorithm):
  def __init__(self, env_name, config):
    Algorithm.__init__(self, env_name, config)
    self.env = create_env(env_name)
    self.policy = LSTMPolicy(
        self.env.observation_space.shape, self.env.action_space.n, 0)
    self.agents = [
        Runner.remote(env_name, i, self.logdir)
        for i in range(config["num_workers"])]
    self.parameters = self.policy.get_weights()
    self.iteration = 0

  def train(self):
    episode_rewards = []
    episode_lengths = []
    gradient_list = [
        agent.compute_gradient.remote(self.parameters)
        for agent in self.agents]
    max_rollouts = self.config["num_rollouts_per_iteration"]
    rollouts_so_far = len(gradient_list)
    while gradient_list:
      done_id, gradient_list = ray.wait(gradient_list)
      gradient, info = ray.get(done_id)[0]
      episode_rewards.append(info["total_reward"])
      episode_lengths.append(info["size"])
      self.policy.model_update(gradient)
      self.parameters = self.policy.get_weights()
      if rollouts_so_far < max_rollouts:
        rollouts_so_far += 1
        gradient_list.extend(
            [self.agents[info["id"]].compute_gradient.remote(self.parameters)])
    res = TrainingResult(
        self.iteration, np.mean(episode_rewards), np.mean(episode_lengths))
    self.iteration += 1
    return res

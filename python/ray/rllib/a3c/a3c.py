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
    "num_batches_per_iteration": 100,
}


@ray.remote
class Runner(object):
    """Actor object to start running simulation on workers.

    The gradient computation is also executed from this object.
    """
    def __init__(self, env_name, policy_cls, actor_id, logdir, start=True):
        env = create_env(env_name)
        self.id = actor_id
        # Todo: should change this to be just env.observation_space
        self.policy = policy_cls(env.observation_space.shape, env.action_space) 
        self.runner = RunnerThread(env, self.policy, 20)
        self.env = env
        self.logdir = logdir
        if start:
            self.start()

    def pull_batch_from_queue(self):
        """Take a rollout from the queue of the thread runner."""
        rollout = self.runner.queue.get(timeout=600.0)
        if isinstance(rollout, BaseException):
            raise rollout
        while not rollout.terminal:
            try:
                part = self.runner.queue.get_nowait()
                if isinstance(part, BaseException):
                    raise rollout
                rollout.extend(part)
            except queue.Empty:
                break
        return rollout

    def get_completed_rollout_metrics(self):
        """Returns metrics on previously completed rollouts.

        Calling this clears the queue of completed rollout metrics.
        """
        completed = []
        while True:
            try:
                completed.append(self.runner.metrics_queue.get_nowait())
            except queue.Empty:
                break
        return completed

    def start(self):
        summary_writer = tf.summary.FileWriter(
            os.path.join(self.logdir, "agent_%d" % self.id))
        self.summary_writer = summary_writer
        self.runner.start_runner(self.policy.sess, summary_writer)

    def compute_gradient(self, params):
        self.policy.set_weights(params)
        rollout = self.pull_batch_from_queue()
        batch = process_rollout(rollout, gamma=0.99, lambda_=1.0)
        gradient, info = self.policy.get_gradients(batch)
        if "summary" in info:
            self.summary_writer.add_summary(tf.Summary.FromString(info['summary']), self.policy.local_steps)
            self.summary_writer.flush()
        info = {"id": self.id,
                "size": len(batch.a)}
        return gradient, info


class A3C(Algorithm):
    def __init__(self, env_name, policy_cls, config, 
                    upload_dir=None):
        config.update({"alg": "A3C"})
        Algorithm.__init__(self, env_name, config, upload_dir=upload_dir) #sets logdir
        self.env = create_env(env_name)
        self.policy = policy_cls(
            self.env.observation_space.shape, self.env.action_space)
        self.agents = [
            Runner.remote(env_name, policy_cls, i, self.logdir)
            for i in range(config["num_workers"])]
        self.parameters = self.policy.get_weights()
        self.iteration = 0

    def train(self):
        gradient_list = [
            agent.compute_gradient.remote(self.parameters)
            for agent in self.agents]
        max_batches = self.config["num_batches_per_iteration"]
        batches_so_far = len(gradient_list)
        while gradient_list:
            done_id, gradient_list = ray.wait(gradient_list)
            gradient, info = ray.get(done_id)[0]
            self.policy.model_update(gradient)
            self.parameters = self.policy.get_weights()
            if batches_so_far < max_batches:
                batches_so_far += 1
                gradient_list.extend(
                    [self.agents[info["id"]].compute_gradient.remote(
                        self.parameters)])
        res = self.fetch_metrics_from_workers()
        self.iteration += 1
        return res

    def fetch_metrics_from_workers(self):
        episode_rewards = []
        episode_lengths = []
        metric_lists = [
            a.get_completed_rollout_metrics.remote() for a in self.agents]
        for metrics in metric_lists:
            for episode in ray.get(metrics):
                episode_lengths.append(episode.episode_length)
                episode_rewards.append(episode.episode_reward)
        res = TrainingResult(
            self.experiment_id.hex, self.iteration,
            np.mean(episode_rewards), np.mean(episode_lengths), dict())
        return res

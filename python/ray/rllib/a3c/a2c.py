from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import tensorflow as tf
import six.moves.queue as queue
import os

import ray
from ray.rllib.a3c.a3c import Runner
from ray.rllib.a3c.envs import create_env
from ray.rllib.common import Algorithm, TrainingResult


DEFAULT_CONFIG = {
    "num_workers": 4,
    "num_batches_per_iteration": 100,
    "batch_size": 10
}

class A2C(Algorithm):
    def __init__(self, env_name, policy_cls, config, upload_dir=None):
        config.update({"alg": "A2C"})
        Algorithm.__init__(self, env_name, config, upload_dir=upload_dir)
        self.env = create_env(env_name)
        self.policy = policy_cls(
            self.env.observation_space.shape, self.env.action_space)
        self.agents = [
            Runner.remote(env_name, policy_cls, i, config["batch_size"], self.logdir)
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
            gradient_list = ray.get(gradient_list)
            gradients, infos = zip(*gradient_list)
            sum_grad = [np.zeros_like(w) for w in gradients[0]]
            for g in gradients:
                for i, node_weight in enumerate(g):
                    sum_grad[i] += node_weight
            self.policy.model_update(sum_grad)
            self.parameters = self.policy.get_weights()
            if batches_so_far < max_batches:
                gradient_list = []
                for agent in self.agents:
                    gradient_list.append(
                        agent.compute_gradient.remote(self.parameters))
                    batches_so_far += 1
            else:
                break
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

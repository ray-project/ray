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


class A2C(Algorithm):
    def __init__(self, env_name, policy_cls, config, upload_dir=None):
        config.update({"alg": "A2C"})
        Algorithm.__init__(self, env_name, config, upload_dir=upload_dir)
        self.env = create_env(env_name)
        self.policy = policy_cls(
            self.env.observation_space.shape, self.env.action_space)
        self.agents = [
            Runner.remote(env_name, policy_cls, i, config["batch_size"], self.logdir, synchronous=True)
            for i in range(config["num_workers"])]
        self.parameters = self.policy.get_weights()
        self.iteration = 0
        self.episode_rewards = []

    def train(self):
        max_batches = self.config["num_batches_per_iteration"]
        batches_so_far = 0
        # batches_so_far = len(gradient_list)
        
        while batches_so_far < max_batches:
            gradient_list = [
                agent.compute_gradient.remote(self.parameters)
                for agent in self.agents]
        #     done, gradient_list = ray.wait(gradient_list)
        #     grad, info = ray.get(done[0])
        #     gradients.append(grad)
        #     gradient_list.append(
        #         self.agents[info['id']].compute_gradient.remote(self.parameters))
            batches_so_far += 1
            gradients, info = zip(*ray.get(gradient_list))
            # gradients.extend(last_batch)
            sum_grad = [np.zeros_like(w) for w in gradients[0]]
            for g in gradients:
                for i, node_weight in enumerate(g):
                    sum_grad[i] += node_weight
            for s in sum_grad:
                s /= len(gradients)
            self.policy.model_update(sum_grad)
            self.parameters = self.policy.get_weights()
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
                self.episode_rewards.append(episode.episode_reward)
        res = TrainingResult(
            self.experiment_id.hex, self.iteration,
            np.mean(episode_rewards), np.mean(episode_lengths), 
            {"last_batch": np.mean(self.episode_rewards[-20:])})
        return res

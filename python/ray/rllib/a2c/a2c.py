from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import tensorflow as tf
import six.moves.queue as queue
import os

import ray
from ray.rllib.a2c.sync_runner import SyncRunner
from ray.rllib.a3c.envs import create_env
from ray.rllib.common import Algorithm, TrainingResult


class A2CAgent(Algorithm):
    def __init__(self, env_name, policy_cls, config, upload_dir=None, summarize=True):
        config.update({"alg": "A2C"})
        Algorithm.__init__(self, env_name, config, upload_dir=upload_dir)
        self.env = create_env(env_name)
        self.policy = policy_cls(
            self.env.observation_space.shape, self.env.action_space, summarize=summarize)
        self.agents = [
            SyncRunner.remote(env_name, policy_cls, i,
                            config["batch_size"], self.logdir)
            for i in range(config["num_workers"])]
        self.parameters = self.policy.get_weights()
        self.summarize = summarize
        if self.summarize:
            self.summary_writer = tf.summary.FileWriter(os.path.join(self.logdir, "driver"))
            self.summary_writer.add_graph(self.policy.g)
        self.iteration = 0
        self.episode_rewards = []

    def train(self):
        """ Implements 1 gradient application """
        max_batches = self.config["num_batches_per_iteration"]
        batches_so_far = 0
        # batches_so_far = len(gradient_list)
        
        while batches_so_far < max_batches:
            gradient_list = [
                agent.compute_gradient.remote(self.parameters)
                for agent in self.agents]
            batches_so_far += 1
            gradients, info = zip(*ray.get(gradient_list))
            # gradients.extend(last_batch)
            sum_grad = [np.zeros_like(w) for w in gradients[0]]
            for g in gradients:
                for i, node_weight in enumerate(g):
                    sum_grad[i] += node_weight
            for s in sum_grad:
                s /= len(gradients)
            info = self.policy.model_update(sum_grad)
            if "summary" in info:
                self.output_summary(info["summary"])
            self.parameters = self.policy.get_weights()
        res = self.fetch_metrics_from_workers()
        self.iteration += 1
        return res

    # def train(self):
    #     """ Implements train with SGD """
    #     max_batches = self.config["num_batches_per_iteration"]
    #     sgd_iters = self.config["num_sgd_iterations"]
    #     batches_so_far = 0
    #     # batches_so_far = len(gradient_list)
    #     while batches_so_far < max_batches:
    #         sample_list = [
    #             agent.compute_samples.remote(self.parameters)
    #             for agent in self.agents]
    #         batches_so_far += 1
    #         samples, info = zip(*ray.get(sample_list))
    #         # gradients.extend(last_batch)
    #         self.policy.run_sgd(samples, sgd_iters)
    #         if "summary" in info:
    #             self.output_summary(info["summary"])
    #         self.parameters = self.policy.get_weights()
    #     res = self.fetch_metrics_from_workers()
    #     self.iteration += 1
    #     return res


    def output_summary(self, summary):
        self.summary_writer.add_summary(
            tf.Summary.FromString(summary), self.policy.num_iter)
        self.summary_writer.flush()

    def _fetch_metrics_from_workers(self):
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
            {"last_batch": np.mean(self.episode_rewards[-10:])})
        return res

    def save(self):
        checkpoint_path = os.path.join(
            self.logdir, "checkpoint-{}".format(self.iteration))
        objects = [
            self.parameters,
            self.iteration]
        pickle.dump(objects, open(checkpoint_path, "wb"))
        return checkpoint_path

    def restore(self, checkpoint_path):
        objects = pickle.load(open(checkpoint_path, "rb"))
        self.parameters = objects[0]
        self.policy.set_weights(self.parameters)
        self.iteration = objects[1]

    def compute_action(self, observation):
        actions = self.policy.compute_actions(observation)[0]
        return actions.argmax()

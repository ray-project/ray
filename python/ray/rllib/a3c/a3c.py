from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import pickle
import tensorflow as tf
import six.moves.queue as queue
import os

import ray
from ray.rllib.a3c.runner import RunnerThread, process_rollout
from ray.rllib.a3c.envs import create_env
from ray.rllib.common import Agent, TrainingResult, get_tensorflow_log_dir
from ray.rllib.a3c.shared_model import SharedModel
from ray.rllib.a3c.shared_model_lstm import SharedModelLSTM


DEFAULT_CONFIG = {
    "num_workers": 4,
    "num_batches_per_iteration": 100,
    "batch_size": 10,
    "use_lstm": True,
    "model": {"grayscale": True,
              "zero_mean": False,
              "dim": 42}
}


@ray.remote
class Runner(object):
    """Actor object to start running simulation on workers.

    The gradient computation is also executed from this object.
    """
    def __init__(self, env_name, policy_cls, actor_id, batch_size,
                 preprocess_config, logdir):
        env = create_env(env_name, preprocess_config)
        self.id = actor_id
        # TODO(rliaw): should change this to be just env.observation_space
        self.policy = policy_cls(env.observation_space.shape, env.action_space)
        self.runner = RunnerThread(env, self.policy, batch_size)
        self.env = env
        self.logdir = logdir
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
        logdir = get_tensorflow_log_dir(self.logdir)
        summary_writer = tf.summary.FileWriter(
            os.path.join(logdir, "agent_%d" % self.id))
        self.summary_writer = summary_writer
        self.runner.start_runner(self.policy.sess, summary_writer)

    def compute_gradient(self, params):
        self.policy.set_weights(params)
        rollout = self.pull_batch_from_queue()
        batch = process_rollout(rollout, gamma=0.99, lambda_=1.0)
        gradient, info = self.policy.get_gradients(batch)
        if "summary" in info:
            self.summary_writer.add_summary(
                tf.Summary.FromString(info['summary']),
                self.policy.local_steps)
            self.summary_writer.flush()
        info = {"id": self.id,
                "size": len(batch.a)}
        return gradient, info


class A3CAgent(Agent):
    def __init__(self, env_name, config, upload_dir=None):
        config.update({"alg": "A3C"})
        Agent.__init__(self, env_name, config, upload_dir=upload_dir)
        self.env = create_env(env_name, config["model"])
        if config.get("use_lstm", True):
            policy_cls = SharedModelLSTM
        else:
            policy_cls = SharedModel
        self.policy = policy_cls(
            self.env.observation_space.shape, self.env.action_space)
        self.agents = [
            Runner.remote(env_name, policy_cls, i,
                          config["batch_size"], config["model"], self.logdir)
            for i in range(config["num_workers"])]
        self.parameters = self.policy.get_weights()

    def _train(self):
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
        res = self._fetch_metrics_from_workers()
        return res

    def _fetch_metrics_from_workers(self):
        episode_rewards = []
        episode_lengths = []
        metric_lists = [
            a.get_completed_rollout_metrics.remote() for a in self.agents]
        for metrics in metric_lists:
            for episode in ray.get(metrics):
                episode_lengths.append(episode.episode_length)
                episode_rewards.append(episode.episode_reward)
        avg_reward = (
            np.mean(episode_rewards) if episode_rewards else float('nan'))
        avg_length = (
            np.mean(episode_lengths) if episode_lengths else float('nan'))
        timesteps = np.sum(episode_lengths) if episode_lengths else 0

        result = TrainingResult(
            episode_reward_mean=avg_reward,
            episode_len_mean=avg_length,
            timesteps_this_iter=timesteps,
            info={})

        return result

    def _save(self):
        checkpoint_path = os.path.join(
            self.logdir, "checkpoint-{}".format(self.iteration))
        objects = [self.parameters]
        pickle.dump(objects, open(checkpoint_path, "wb"))
        return checkpoint_path

    def _restore(self, checkpoint_path):
        objects = pickle.load(open(checkpoint_path, "rb"))
        self.parameters = objects[0]
        self.policy.set_weights(self.parameters)

    def compute_action(self, observation):
        actions = self.policy.compute_actions(observation)
        return actions[0]

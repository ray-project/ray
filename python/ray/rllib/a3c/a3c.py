from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import pickle
import os

import ray
from ray.rllib.agent import Agent
from ray.rllib.optimizers import AsyncOptimizer
from ray.rllib.utils import FilterManager
from ray.rllib.a3c.a3c_evaluator import A3CEvaluator, RemoteA3CEvaluator, \
    GPURemoteA3CEvaluator
from ray.tune.result import TrainingResult


DEFAULT_CONFIG = {
    # Number of workers (excluding master)
    "num_workers": 4,
    # Size of rollout batch
    "batch_size": 10,
    # Use LSTM model - only applicable for image states
    "use_lstm": False,
    # Use PyTorch as backend - no LSTM support
    "use_pytorch": False,
    # Which observation filter to apply to the observation
    "observation_filter": "NoFilter",
    # Which reward filter to apply to the reward
    "reward_filter": "NoFilter",
    # Discount factor of MDP
    "gamma": 0.99,
    # GAE(gamma) parameter
    "lambda": 1.0,
    # Max global norm for each gradient calculated by worker
    "grad_clip": 40.0,
    # Learning rate
    "lr": 0.0001,
    # Value Function Loss coefficient
    "vf_loss_coeff": 0.5,
    # Entropy coefficient
    "entropy_coeff": -0.01,
    # Whether to place workers on GPUs
    "use_gpu_for_workers": False,
    # Model and preprocessor options
    "model": {
        # (Image statespace) - Converts image to Channels = 1
        "grayscale": True,
        # (Image statespace) - Each pixel
        "zero_mean": False,
        # (Image statespace) - Converts image to (dim, dim, C)
        "dim": 80,
        # (Image statespace) - Converts image shape to (C, dim, dim)
        "channel_major": False
    },
    # Arguments to pass to the rllib optimizer
    "optimizer": {
        # Number of gradients applied for each `train` step
        "grads_per_step": 100,
    },
    # Arguments to pass to the env creator
    "env_config": {},
}


class A3CAgent(Agent):
    _agent_name = "A3C"
    _default_config = DEFAULT_CONFIG
    _allow_unknown_subkeys = ["model", "optimizer", "env_config"]

    def _init(self):
        self.local_evaluator = A3CEvaluator(
            self.registry, self.env_creator, self.config, self.logdir,
            start_sampler=False)
        if self.config["use_gpu_for_workers"]:
            remote_cls = GPURemoteA3CEvaluator
        else:
            remote_cls = RemoteA3CEvaluator
        self.remote_evaluators = [
            remote_cls.remote(
                self.registry, self.env_creator, self.config, self.logdir)
            for i in range(self.config["num_workers"])]
        self.optimizer = AsyncOptimizer(
            self.config["optimizer"], self.local_evaluator,
            self.remote_evaluators)

    def _train(self):
        self.optimizer.step()
        FilterManager.synchronize(
            self.local_evaluator.filters, self.remote_evaluators)
        res = self._fetch_metrics_from_remote_evaluators()
        return res

    def _fetch_metrics_from_remote_evaluators(self):
        episode_rewards = []
        episode_lengths = []
        metric_lists = [a.get_completed_rollout_metrics.remote()
                        for a in self.remote_evaluators]
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

    def _save(self, checkpoint_dir):
        checkpoint_path = os.path.join(
            checkpoint_dir, "checkpoint-{}".format(self.iteration))
        agent_state = ray.get(
            [a.save.remote() for a in self.remote_evaluators])
        extra_data = {
            "remote_state": agent_state,
            "local_state": self.local_evaluator.save()}
        pickle.dump(extra_data, open(checkpoint_path + ".extra_data", "wb"))
        return checkpoint_path

    def _restore(self, checkpoint_path):
        extra_data = pickle.load(open(checkpoint_path + ".extra_data", "rb"))
        ray.get(
            [a.restore.remote(o) for a, o in zip(
                self.remote_evaluators, extra_data["remote_state"])])
        self.local_evaluator.restore(extra_data["local_state"])

    def compute_action(self, observation):
        obs = self.local_evaluator.obs_filter(observation, update=False)
        action, info = self.local_evaluator.policy.compute(obs)
        return action

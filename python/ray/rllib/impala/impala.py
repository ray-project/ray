from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np

import ray
from ray.rllib.agent import Agent
from ray.rllib.impala.impala_evaluator import ImpalaEvaluator, RemoteImpalaEvaluator
from ray.rllib.optimizers import ImpalaOptimizer
from ray.tune.result import TrainingResult

DEFAULT_CONFIG = {
    # learning rate
    "lr": 0.0006,
    # If not None, clip gradients during optimization at this value
    'grad_norm_clipping': 40,
    # Baseline loss scaling
    "baseline_weight": 0.5,
    # Entropy regularizater
    "entropy_weight": 0.01,
    # Arguments to pass in to env creator
    "env_config": {},
    # MDP Discount factor
    "gamma": 0.99,
    # Number of steps after which the rollout gets cut
    "horizon": None,

    # Number of local steps taken for each call to sample
    "num_local_steps": 20,
    # Number of workers (excluding master)
    "num_workers": 24,

    "optimizer": {
        # Whether to clip rewards
        "clip_rewards": True,
        # Size of batch sampled from replay buffer
        "train_batch_size": 32,
    },

    # Number of steps taken per training iteration
    "train_steps": 600,
}


class ImpalaAgent(Agent):
    _agent_name = "Impala"
    _default_config = DEFAULT_CONFIG

    def _init(self):
        self.local_evaluator = ImpalaEvaluator(
            self.registry, self.env_creator, self.config)
        self.remote_evaluators = [
            RemoteImpalaEvaluator.remote(
                self.registry, self.env_creator, self.config)
            for _ in range(self.config["num_workers"])]
        self.optimizer = ImpalaOptimizer(
            self.config["optimizer"], self.local_evaluator,
            self.remote_evaluators)

    def _train(self):
        for _ in range(self.config["train_steps"]):
            self.optimizer.step()

        # generate training result
        return self._fetch_metrics()

    def _fetch_metrics(self):
        episode_rewards = []
        episode_lengths = []
        if self.config["num_workers"] > 0:
            metric_lists = [a.get_completed_rollout_metrics.remote()
                            for a in self.remote_evaluators]
            for metrics in metric_lists:
                for episode in ray.get(metrics):
                    episode_lengths.append(episode.episode_length)
                    episode_rewards.append(episode.episode_reward)
        else:
            metrics = self.local_evaluator.get_completed_rollout_metrics()
            for episode in metrics:
                episode_lengths.append(episode.episode_length)
                episode_rewards.append(episode.episode_reward)

        avg_reward = (np.mean(episode_rewards))
        avg_length = (np.mean(episode_lengths))
        timesteps = np.sum(episode_lengths)

        result = TrainingResult(
            episode_reward_mean=avg_reward,
            episode_len_mean=avg_length,
            timesteps_this_iter=timesteps,
            info={})

        return result

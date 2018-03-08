# imports
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray
from ray.rllib.optimizers import LocalSyncOptimizer
from ray.rllib.agent import Agent
from ray.rllib.ddpg.ddpg_evaluator import DDPGEvaluator, RemoteDDPGEvaluator
from ray.tune.result import TrainingResult
import numpy as np

# add more stuff to config as necessary
DEFAULT_CONFIG = {
    "actor_model": {},
    "batch_size": 256,
    "buffer_size": 1000000,
    "critic_model": {},
    "env_config": {},
    # Discount factor
    "gamma": 0.99,
    "horizon": 500,
    "actor_lr": 0.0001,
    "critic_lr": 0.001,
    #"model": {},
    "num_workers": 1,
    # Arguments to pass to the rllib optimizer
    "optimizer": {
        # Number of gradients applied for each `train` step
        "grads_per_step": 1,
    },
    "train_batch_size": 10,
    "tau": 0.01,
}

class DDPGAgent(Agent):
    _agent_name = "DDPG"
    _default_config = DEFAULT_CONFIG

    def _init(self):
        self.local_evaluator = DDPGEvaluator(
            self.registry, self.env_creator, self.config)
        self.remote_evaluators = [
            RemoteDDPGEvaluator.remote(
                self.registry, self.env_creator, self.config)
            for _ in range(self.config["num_workers"])]
        self.optimizer = LocalSyncOptimizer(
            self.config["optimizer"], self.local_evaluator,
            self.remote_evaluators)

    def _train(self):
        self.optimizer.step()
        # update target
        self.local_evaluator.update_target()
        # generate training result

        episode_rewards = []
        episode_lengths = []
        metric_lists = [a.get_completed_rollout_metrics.remote()
                        for a in self.remote_evaluators]
        for metrics in metric_lists:
            for episode in ray.get(metrics):
                episode_lengths.append(episode.episode_length)
                episode_rewards.append(episode.episode_reward)
        avg_reward = np.mean(episode_rewards)
        avg_length = np.mean(episode_lengths)
        timesteps = np.sum(episode_lengths)

        result = TrainingResult(
            episode_reward_mean=avg_reward,
            episode_len_mean=avg_length,
            timesteps_this_iter=timesteps,
            info={})

        return result

    def compute_action(self, obs):
        # select action using actor
        action, info = self.local_evaluator.actor.act(obs)
        return action

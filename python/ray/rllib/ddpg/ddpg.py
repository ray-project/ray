from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np

import ray
from ray.rllib.agent import Agent
from ray.rllib.ddpg.ddpg_evaluator import DDPGEvaluator, RemoteDDPGEvaluator
from ray.rllib.optimizers import LocalSyncReplayOptimizer
from ray.tune.result import TrainingResult

DEFAULT_CONFIG = {
    # Actor learning rate
    "actor_lr": 0.0001,
    # Critic learning rate
    "critic_lr": 0.001,
    # Arguments to pass in to env creator
    "env_config": {},
    # MDP Discount factor
    "gamma": 0.99,
    # Number of steps after which the rollout gets cut
    "horizon": 500,

    # Whether to include parameter noise
    "noise_add": True,
    # Linear decay of exploration policy
    "noise_epsilon": 0.0002,
    # Parameters for noise process
    "noise_parameters": {
        "mu": 0,
        "sigma": 0.2,
        "theta": 0.15,
    },

    # Number of local steps taken for each call to sample
    "num_local_steps": 1,
    # Number of workers (excluding master)
    "num_workers": 0,

    "optimizer": {
        # Replay buffer size
        "buffer_size": 10000,
        # Number of steps in warm-up phase before learning starts
        "learning_starts": 500,
        # Whether to clip rewards
        "clip_rewards": False,
        # Whether to use prioritized replay
        "prioritized_replay": False,
        # Size of batch sampled from replay buffer
        "train_batch_size": 64,
    },

    # Controls how fast target networks move
    "tau": 0.001,
    # Number of steps taken per training iteration
    "train_steps": 600,
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
        self.optimizer = LocalSyncReplayOptimizer(
            self.config["optimizer"], self.local_evaluator,
            self.remote_evaluators)

    def _train(self):
        for _ in range(self.config["train_steps"]):
            self.optimizer.step()
            # update target
            if self.optimizer.num_steps_trained > 0:
                self.local_evaluator.update_target()

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

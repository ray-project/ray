import numpy as np

import ray
from ray.rllib.optimizers import LocalSyncOptimizer
from ray.rllib.pg.pg_evaluator import PGEvaluator, RemotePGEvaluator
from ray.rllib.agent import Agent
from ray.rllib.utils import FilterManager
from ray.tune.result import TrainingResult

DEFAULT_CONFIG = {
    # Number of workers (excluding master)
    "num_workers": 4,
    # Size of rollout batch
    "batch_size": 512,
    # Use LSTM model - only applicable for image states
    "observation_filter": "NoFilter",
    # Which reward filter to apply to the reward
    "reward_filter": "NoFilter",
    # Discount factor of MDP
    "gamma": 0.99,
    # GAE(gamma) parameter
    "lambda": 1.0,
    # Max global norm for each gradient calculated by worker
    "grad_clip": 10.0,
    # Learning rate
    "lr": 0.0001,
    # Value Function Loss coefficient
    "vf_loss_coeff": 0.5,
    # Model and preprocessor options
    "model": {},
    # Arguments to pass to the rllib optimizer
    "optimizer": {
        # Number of gradients applied for each `train` step
        "grads_per_step": 10,
    },
    "model_options": {},
    # Arguments to pass to the env creator
    "env_config": {},
}

class PGAgent(Agent):
    _agent_name = "PG"
    _default_config = DEFAULT_CONFIG

    def _init(self):
        self.local_evaluator = PGEvaluator(
            self.registry, self.env_creator, self.config)
        self.remote_evaluators = [
            RemotePGEvaluator.remote(
                self.registry, self.env_creator, self.config)
            for _ in range(self.config["num_workers"])]
        self.optimizer = LocalSyncOptimizer(
            self.config["optimizer"], self.local_evaluator,
            self.remote_evaluators)

    def _train(self):
        self.optimizer.step()

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

    def compute_action(self, observation):
        obs = self.local_evaluator.obs_filter(observation, update=False)
        action, info = self.local_evaluator.policy.compute(obs)
        return action

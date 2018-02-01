from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np

import ray
from ray.rllib.optimizers import LocalSyncOptimizer
from ray.rllib.pg.pg_evaluator import PGEvaluator
from ray.rllib.agent import Agent
from ray.tune.result import TrainingResult

DEFAULT_CONFIG = {
    # Number of workers (excluding master)
    "num_workers": 4,
    # Size of rollout batch
    "batch_size": 512,
    # Discount factor of MDP
    "gamma": 0.99,
    # Number of steps after which the rollout gets cut
    "horizon": 500,
    # Learning rate
    "lr": 0.0004,
    # Arguments to pass to the rllib optimizer
    "optimizer": {},
    # Model parameters
    "model": {"fcnet_hiddens": [128, 128]},
    # Arguments to pass to the env creator
    "env_config": {},
}


class PGAgent(Agent):

    """Simple policy gradient agent.

    This is an example agent to show how to implement algorithms in RLlib.
    In most cases, you will probably want to use the PPO agent instead.
    """

    _agent_name = "PG"
    _default_config = DEFAULT_CONFIG

    def _init(self):
        self.optimizer = LocalSyncOptimizer.make(
            evaluator_cls=PGEvaluator,
            evaluator_args=[self.registry, self.env_creator, self.config],
            num_workers=self.config["num_workers"],
            optimizer_config=self.config["optimizer"])

    def _train(self):
        self.optimizer.step()

        episode_rewards = []
        episode_lengths = []
        metric_lists = [a.get_completed_rollout_metrics.remote()
                        for a in self.optimizer.remote_evaluators]
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
        action, info = self.optimizer.local_evaluator.policy.compute(obs)
        return action

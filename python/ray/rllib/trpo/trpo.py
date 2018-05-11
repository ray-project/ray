from __future__ import absolute_import, division, print_function

import numpy as np
import ray
from ray.tune.trial import Resources
from ray.rllib.agent import Agent
from ray.rllib.optimizers import LocalSyncOptimizer
from ray.tune.result import TrainingResult

from ray.rllib.trpo.trpo_evaluator import TRPOEvaluator

DEFAULT_CONFIG = {
    # Number of workers (excluding master)
    'num_workers': 4,
    'use_gpu_for_workers': False,
    'vf_loss_coeff': 0.5,
    'use_lstm': False,
    # Size of rollout batch
    'batch_size': 512,
    # Discount factor of MDP
    'gamma': 0.99,
    'use_pytorch': True,
    'observation_filter': 'NoFilter',
    'reward_filter': 'NoFilter',
    'ent_coeff': 0.0,
    'max_kl': 0.001,
    'cg_damping': 0.001,
    'residual_tol': 1e-10,

    # Number of steps after which the rollout gets cut
    'horizon': 500,
    # Learning rate
    'lr': 0.0004,
    # Arguments to pass to the RLlib optimizer
    'optimizer': {},
    # Model parameters
    'model': {
        'fcnet_hiddens': [128, 128]
    },
    # Arguments to pass to the env creator
    'env_config': {},
}


class TRPOAgent(Agent):
    _agent_name = 'TRPO'
    _default_config = DEFAULT_CONFIG
    _allow_unknown_subkeys = ['model', 'optimizer', 'env_config']

    @classmethod
    def default_resource_request(cls, config):
        cf = dict(cls._default_config, **config)
        return Resources(
            cpu=1,
            gpu=0,
            extra_cpu=cf["num_workers"],
            extra_gpu=cf["use_gpu_for_workers"] and cf["num_workers"] or 0)

    def _init(self):

        self.local_evaluator = TRPOEvaluator(
            self.registry,
            self.env_creator,
            self.config,
        )

        RemoteTRPOEvaluator = ray.remote(TRPOEvaluator)

        self.remote_evaluators = [
            RemoteTRPOEvaluator.remote(
                self.registry,
                self.env_creator,
                self.config,
            ) for _ in range(self.config['num_workers'])
        ]

        self.optimizer = LocalSyncOptimizer.make(
            evaluator_cls=TRPOEvaluator,
            evaluator_args=[self.registry, self.env_creator, self.config],
            num_workers=self.config['num_workers'],
            optimizer_config=self.config['optimizer'],
        )

    def _train(self):
        self.optimizer.step()

        episode_rewards = []
        episode_lengths = []

        metric_lists = [
            a.get_completed_rollout_metrics.remote()
            for a in self.optimizer.remote_evaluators
        ]

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
            info={},
        )

        return result

    # TODO(alok): implement `_stop`, `_save`, `_restore`

    def compute_action(self, observation):
        obs = self.local_evaluator.obs_filter(observation, update=False)
        action, _ = self.local_evaluator.policy.compute(obs)
        return action

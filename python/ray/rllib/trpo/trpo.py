from __future__ import absolute_import, division, print_function

import os
import pickle

import numpy as np

import ray
from ray.rllib.agent import Agent
from ray.rllib.optimizers import AsyncOptimizer, LocalSyncOptimizer
from ray.rllib.trpo.trpo_evaluator import TRPOEvaluator
from ray.rllib.utils import FilterManager
from ray.tune.result import TrainingResult
from ray.tune.trial import Resources

DEFAULT_CONFIG = {
    # Number of workers (excluding master)
    'num_workers': 4,
    'use_gpu_for_workers': False,
    'vf_loss_coeff': 0.5,
    'use_lstm': False,
    # Size of rollout
    'batch_size': 512,
    # GAE(gamma) parameter
    'lambda': 1.0,
    # Max global norm for each gradient calculated by worker
    'grad_clip': 40.0,
    # Discount factor of MDP
    'gamma': 0.99,
    'use_pytorch': True,
    'observation_filter': 'NoFilter',
    'reward_filter': 'NoFilter',
    'entropy_coeff': 0.0,
    'max_kl': 0.001,
    'cg_damping': 0.001,
    'residual_tol': 1e-10,
    # Number of steps after which the rollout gets cut
    'horizon': 500,
    # Learning rate
    'lr': 0.0001,
    # Arguments to pass to the RLlib optimizer
    'optimizer': {
        # Number of gradients applied for each `train` step
        'grads_per_step': 100,
    },
    # Model and preprocessor options
    'model': {
        # (Image statespace) - Converts image to Channels = 1
        'grayscale': True,
        # (Image statespace) - Each pixel
        'zero_mean': False,
        # (Image statespace) - Converts image to (dim, dim, C)
        'dim': 80,
        # (Image statespace) - Converts image shape to (C, dim, dim)
        #
        # XXX set to true by default here since there's currently only a
        # PyTorch implementation.
        "channel_major": True,
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
            extra_cpu=cf['num_workers'],
            extra_gpu=cf['use_gpu_for_workers'] and cf['num_workers'] or 0)

    def _init(self):
        self.local_evaluator = TRPOEvaluator(
            self.registry,
            self.env_creator,
            self.config,
            self.logdir,
            start_sampler=False,
        )

        RemoteTRPOEvaluator = ray.remote(TRPOEvaluator)

        self.remote_evaluators = [
            RemoteTRPOEvaluator.remote(
                self.registry,
                self.env_creator,
                self.config,
                self.logdir,
                start_sampler=True,
            ) for _ in range(self.config['num_workers'])
        ]

        self.optimizer = AsyncOptimizer(self.config['optimizer'],
                                        self.local_evaluator,
                                        self.remote_evaluators)

    def _train(self):
        self.optimizer.step()
        FilterManager.synchronize(self.local_evaluator.filters,
                                  self.remote_evaluators)
        result = self._fetch_metrics_from_remote_evaluators()
        return result

    def _fetch_metrics_from_remote_evaluators(self):
        episode_rewards = []
        episode_lengths = []

        metric_lists = [
            a.get_completed_rollout_metrics.remote()
            for a in self.remote_evaluators
        ]

        for metrics in metric_lists:
            for episode in ray.get(metrics):
                episode_lengths.append(episode.episode_length)
                episode_rewards.append(episode.episode_reward)

        avg_reward = (np.mean(episode_rewards)
                      if episode_rewards else float('nan'))
        avg_length = (np.mean(episode_lengths)
                      if episode_lengths else float('nan'))
        timesteps = np.sum(episode_lengths) if episode_lengths else 0

        result = TrainingResult(
            episode_reward_mean=avg_reward,
            episode_len_mean=avg_length,
            timesteps_this_iter=timesteps,
            info={})

        return result

    def _stop(self):
        # workaround for https://github.com/ray-project/ray/issues/1516
        for ev in self.remote_evaluators:
            ev.__ray_terminate__.remote()

    def _save(self, checkpoint_dir):
        checkpoint_path = os.path.join(checkpoint_dir,
                                       'checkpoint-{}'.format(self.iteration))
        agent_state = ray.get(
            [a.save.remote() for a in self.remote_evaluators])

        extra_data = {
            'remote_state': agent_state,
            'local_state': self.local_evaluator.save()
        }

        pickle.dump(extra_data, open(checkpoint_path + '.extra_data', 'wb'))

        return checkpoint_path

    def _restore(self, checkpoint_path):
        extra_data = pickle.load(open(checkpoint_path + '.extra_data', 'rb'))

        ray.get([
            a.restore.remote(o)
            for a, o in zip(self.remote_evaluators, extra_data['remote_state'])
        ])

        self.local_evaluator.restore(extra_data['local_state'])

    def compute_action(self, observation):
        obs = self.local_evaluator.obs_filter(observation, update=False)
        action, _ = self.local_evaluator.policy.compute(obs)
        return action

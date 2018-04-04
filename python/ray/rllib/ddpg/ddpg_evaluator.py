# imports
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray
from ray.rllib.ddpg.models import DDPGModel
from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.optimizers import PolicyEvaluator
from ray.rllib.utils.filter import NoFilter
from ray.rllib.utils.process_rollout import process_rollout
from ray.rllib.utils.sampler import SyncSampler


import numpy as np


class DDPGEvaluator(PolicyEvaluator):

    def __init__(self, registry, env_creator, config):
        self.registry = registry
        self.env = ModelCatalog.get_preprocessor_as_wrapper(
            registry, env_creator(config["env_config"]))
        self.config = config

        # contains model, target_model
        self.model = DDPGModel(registry, self.env, config)

        self.sampler = SyncSampler(
                        self.env, self.model.model, NoFilter(),
                        config["num_local_steps"], horizon=config["horizon"])

        self.episode_rewards = [0.0]
        self.episode_lengths = [0.0]

    def sample(self):
        """Returns a batch of samples."""

        rollout = self.sampler.get_data()
        rollout.data["weights"] = np.ones_like(rollout.data["rewards"])

        self.episode_rewards[-1] += rollout.data["rewards"][0]
        self.episode_lengths[-1] += 1
        if rollout.data["dones"][0]:
            print(len(self.episode_rewards), self.episode_rewards[-1])
            self.episode_rewards.append(0.0)
            self.episode_lengths.append(0.0)

        samples = process_rollout(
                    rollout, NoFilter(),
                    gamma=1.0, use_gae=False)

        return samples

    def stats(self):
        n = self.config["smoothing_num_episodes"] + 1
        mean_10ep_reward = round(np.mean(self.episode_rewards[-n:-1]), 5)
        mean_10ep_length = round(np.mean(self.episode_lengths[-n:-1]), 5)
        return {
            "mean_10ep_reward": mean_10ep_reward,
            "mean_10ep_length": mean_10ep_length,
            "num_episodes": len(self.episode_rewards),
        }

    def update_target(self):
        """Updates target critic and target actor."""
        self.model.update_target()

    def compute_gradients(self, samples):
        """Returns critic, actor gradients."""
        return self.model.compute_gradients(samples)

    def apply_gradients(self, grads):
        """Applies gradients to evaluator weights."""
        self.model.apply_gradients(grads)

    def compute_apply(self, samples):
        grads, _ = self.compute_gradients(samples)
        self.apply_gradients(grads)

    def get_weights(self):
        """Returns model weights."""
        return self.model.get_weights()

    def set_weights(self, weights):
        """Sets model weights."""
        self.model.set_weights(weights)

    def get_completed_rollout_metrics(self):
        """Returns metrics on previously completed rollouts.

        Calling this clears the queue of completed rollout metrics.
        """
        return self.sampler.get_metrics()


RemoteDDPGEvaluator = ray.remote(DDPGEvaluator)

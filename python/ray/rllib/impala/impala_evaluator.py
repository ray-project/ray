from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np

import ray
from ray.rllib.impala.policy import Policy
from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.optimizers import PolicyEvaluator
from ray.rllib.utils.filter import NoFilter
from ray.rllib.utils.process_rollout import process_rollout
from ray.rllib.utils.sampler import SyncSampler


class ImpalaEvaluator(PolicyEvaluator):

    def __init__(self, registry, env_creator, config):
        self.env = ModelCatalog.get_preprocessor_as_wrapper(
            registry, env_creator(config["env_config"]), config["model"])

        # contains model, target_model
        self.policy = Policy(
            registry, self.env.observation_space.shape, self.env.action_space, config)
        self.config = config

        self.obs_filter = get_filter(
            config["observation_filter"], self.env.observation_space.shape)
        self.rew_filter = get_filter(
            config["reward_filter"], ())
        self.sampler = SyncSampler(
                        self.env, self.model.model, self.obs_filter,
                        config["num_local_steps"], horizon=config["horizon"])

    def sample(self):
        rollout = self.sampler.get_data()
        samples = process_rollout(
                    rollout, self.rew_filter, self.config["gamma"],
                    self.config["lambda"], use_gae=False)
        return samples

    def compute_gradients(self, samples):
        return self.model.compute_gradients(samples)

    def apply_gradients(self, grads):
        self.model.apply_gradients(grads)

    def compute_apply(self, samples):
        grads, _ = self.compute_gradients(samples)
        self.apply_gradients(grads)

    def get_weights(self):
        return self.model.get_weights()

    def set_weights(self, weights):
        self.model.set_weights(weights)

    def get_completed_rollout_metrics(self):
        """Returns metrics on previously completed rollouts.

        Calling this clears the queue of completed rollout metrics.
        """
        return self.sampler.get_metrics()


RemoteImpalaEvaluator = ray.remote(ImpalaEvaluator)

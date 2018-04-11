from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np

import ray
from ray.rllib.ddpg.models import DDPGModel
from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.optimizers import PolicyEvaluator
from ray.rllib.utils.filter import NoFilter
from ray.rllib.utils.process_rollout import process_rollout
from ray.rllib.utils.sampler import SyncSampler


class DDPGEvaluator(PolicyEvaluator):

    def __init__(self, registry, env_creator, config):
        self.env = ModelCatalog.get_preprocessor_as_wrapper(
            registry, env_creator(config["env_config"]))

        # contains model, target_model
        self.model = DDPGModel(registry, self.env, config)

        self.sampler = SyncSampler(
                        self.env, self.model.model, NoFilter(),
                        config["num_local_steps"], horizon=config["horizon"])

    def sample(self):
        """Returns a batch of samples."""

        rollout = self.sampler.get_data()
        rollout.data["weights"] = np.ones_like(rollout.data["rewards"])

        # since each sample is one step, no discounting needs to be applied;
        # this does not involve config["gamma"]
        samples = process_rollout(
                    rollout, NoFilter(),
                    gamma=1.0, use_gae=False)

        return samples

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

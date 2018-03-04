from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.optimizers import Evaluator
from ray.rllib.pg.policy import PGPolicy
from ray.rllib.utils.filter import NoFilter
from ray.rllib.utils.process_rollout import process_rollout
from ray.rllib.utils.sampler import SyncSampler


class PGEvaluator(Evaluator):
    """Evaluator for simple policy gradient."""

    def __init__(self, registry, env_creator, config):
        self.env = ModelCatalog.get_preprocessor_as_wrapper(
            registry, env_creator(config["env_config"]), config["model"])
        self.config = config

        self.policy = PGPolicy(registry, self.env.observation_space,
                               self.env.action_space, config)
        self.sampler = SyncSampler(
                        self.env, self.policy, NoFilter(),
                        config["batch_size"], horizon=config["horizon"])

    def sample(self):
        rollout = self.sampler.get_data()
        samples = process_rollout(
                    rollout, NoFilter(),
                    gamma=self.config["gamma"], use_gae=False)
        return samples

    def get_completed_rollout_metrics(self):
        """Returns metrics on previously completed rollouts.

        Calling this clears the queue of completed rollout metrics.
        """
        return self.sampler.get_metrics()

    def compute_gradients(self, samples):
        """ Returns gradient w.r.t. samples."""
        gradient, info = self.policy.compute_gradients(samples)
        return gradient

    def apply_gradients(self, grads):
        """Applies gradients to evaluator weights."""
        self.policy.apply_gradients(grads)

    def get_weights(self):
        """Returns model weights."""
        return self.policy.get_weights()

    def set_weights(self, weights):
        """Sets model weights."""
        return self.policy.set_weights(weights)

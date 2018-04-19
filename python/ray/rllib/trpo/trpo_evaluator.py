from __future__ import absolute_import, division, print_function

from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.optimizers import PolicyEvaluator
from ray.rllib.utils.filter import NoFilter
from ray.rllib.utils.process_rollout import process_rollout
from ray.rllib.utils.sampler import SyncSampler

# TODO use from ray.rllib.trpo.policy import TRPOPolicy
from policy import TRPOPolicy


class TRPOEvaluator(PolicyEvaluator):
    def __init__(self, registry, env_creator, config):

        self.env = ModelCatalog.get_preprocessor_as_wrapper(
            registry,
            env=env_creator(config["env_config"]),
            options=config["model"],
        )

        self.config = config

        self.policy = TRPOPolicy(
            registry,
            self.env.observation_space,
            self.env.action_space,
            config,
        )

        self.sampler = SyncSampler(
            self.env,
            self.policy,
            obs_filter=NoFilter(),
            num_local_steps=config["batch_size"],
            horizon=config["horizon"],
        )

    def sample(self):
        rollout = self.sampler.get_data()

        samples = process_rollout(
            rollout,
            NoFilter(),
            gamma=self.config["gamma"],
            use_gae=False,
        )

        return samples

    def get_completed_rollout_metrics(self):
        """Returns metrics on previously completed rollouts.

        Calling this clears the queue of completed rollout metrics.
        """
        return self.sampler.get_metrics()

    def compute_gradients(self, samples):
        """Returns gradient w.r.t.

        samples.
        """
        gradient, info = self.policy.compute_gradients(samples)
        return gradient, {}

    def apply_gradients(self, grads):
        """Applies gradients to evaluator weights."""

        self.policy.apply_gradients(grads)

    def get_weights(self):
        """Returns model weights."""

        return self.policy.get_weights()

    def set_weights(self, weights):
        """Sets model weights."""

        return self.policy.set_weights(weights)

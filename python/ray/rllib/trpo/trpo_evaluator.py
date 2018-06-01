from __future__ import absolute_import, division, print_function

import pickle

from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.optimizers import PolicyEvaluator
from ray.rllib.trpo.policy import TRPOPolicy
from ray.rllib.utils.filter import NoFilter, get_filter
from ray.rllib.utils.process_rollout import process_rollout
from ray.rllib.utils.sampler import SyncSampler


class TRPOEvaluator(PolicyEvaluator):
    """Actor object to start running simulation on workers.

    The gradient computation is also executed from this object.

    Attributes:
        policy: Copy of graph used for policy. Used by sampler and gradients.
        obs_filter: Observation filter used in environment sampling
        rew_filter: Reward filter used in rollout post-processing.
        sampler: Component for interacting with environment and generating
            rollouts.
    """

    def __init__(
            self,
            registry,
            env_creator,
            config,
    ):
        self.config = config

        self.env = ModelCatalog.get_preprocessor_as_wrapper(
            registry,
            env=env_creator(self.config['env_config']),
            options=self.config['model'],
        )

        # TODO(alok): use ob_space directly rather than shape
        self.policy = TRPOPolicy(
            registry,
            self.env.observation_space.shape,
            self.env.action_space,
            self.config,
        )

        self.obs_filter = get_filter(
            self.config['observation_filter'],
            self.env.observation_space.shape,
        )
        self.rew_filter = get_filter(self.config['reward_filter'], ())
        self.filters = {
            'obs_filter': self.obs_filter,
            'rew_filter': self.rew_filter,
        }

        self.sampler = SyncSampler(
            self.env,
            self.policy,
            obs_filter=NoFilter(),
            num_local_steps=config['batch_size'],
            horizon=config['horizon'],
        )

    def sample(self):
        rollout = self.sampler.get_data()

        samples = process_rollout(
            rollout,
            NoFilter(),
            gamma=self.config['gamma'],
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
        gradient, _ = self.policy.compute_gradients(samples)
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

    def save(self):
        filters = self.get_filters(flush_after=True)
        weights = self.get_weights()
        return pickle.dumps({"filters": filters, "weights": weights})

    def restore(self, objs):
        objs = pickle.loads(objs)
        self.sync_filters(objs["filters"])
        self.set_weights(objs["weights"])

    def sync_filters(self, new_filters):
        """Changes self's filter to given and rebases any accumulated delta.

        Args:
            new_filters (dict): Filters with new state to update local copy.
        """
        assert all(k in new_filters for k in self.filters)
        for k in self.filters:
            self.filters[k].sync(new_filters[k])

    def get_filters(self, flush_after=False):
        """Returns a snapshot of filters.

        Args:
            flush_after (bool): Clears the filter buffer state.

        Returns:
            return_filters (dict): Dict for serializable filters
        """
        return_filters = {}
        for k, f in self.filters.items():
            return_filters[k] = f.as_serializable()
            if flush_after:
                f.clear_buffer()
        return return_filters

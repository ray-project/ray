from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import pickle

import ray
from ray.rllib.models import ModelCatalog
from ray.rllib.optimizers import Evaluator
from ray.rllib.a3c.common import get_policy_cls
from ray.rllib.utils.filter import get_filter
from ray.rllib.utils.sampler import AsyncSampler
from ray.rllib.utils.process_rollout import process_rollout


class A3CEvaluator(Evaluator):
    """Actor object to start running simulation on workers.

    The gradient computation is also executed from this object.

    Attributes:
        policy: Copy of graph used for policy. Used by sampler and gradients.
        obs_filter: Observation filter used in environment sampling
        rew_filter: Reward filter used in rollout post-processing.
        sampler: Component for interacting with environment and generating
            rollouts.
        logdir: Directory for logging.
    """
    def __init__(
            self, registry, env_creator, config, logdir, start_sampler=True):
        env = ModelCatalog.get_preprocessor_as_wrapper(
            registry, env_creator(config["env_config"]), config["model"])
        self.env = env
        policy_cls = get_policy_cls(config)
        # TODO(rliaw): should change this to be just env.observation_space
        self.policy = policy_cls(
            registry, env.observation_space.shape, env.action_space, config)
        self.config = config

        # Technically not needed when not remote
        self.obs_filter = get_filter(
            config["observation_filter"], env.observation_space.shape)
        self.rew_filter = get_filter(config["reward_filter"], ())
        self.filters = {"obs_filter": self.obs_filter,
                        "rew_filter": self.rew_filter}
        self.sampler = AsyncSampler(env, self.policy, self.obs_filter,
                                    config["batch_size"])
        if start_sampler and self.sampler.async:
            self.sampler.start()
        self.logdir = logdir

    def sample(self):
        rollout = self.sampler.get_data()
        samples = process_rollout(
            rollout, self.rew_filter, gamma=self.config["gamma"],
            lambda_=self.config["lambda"], use_gae=True)
        return samples

    def get_completed_rollout_metrics(self):
        """Returns metrics on previously completed rollouts.

        Calling this clears the queue of completed rollout metrics.
        """
        return self.sampler.get_metrics()

    def compute_gradients(self, samples):
        gradient, info = self.policy.compute_gradients(samples)
        return gradient

    def apply_gradients(self, grads):
        self.policy.apply_gradients(grads)

    def get_weights(self):
        return self.policy.get_weights()

    def set_weights(self, params):
        self.policy.set_weights(params)

    def save(self):
        filters = self.get_filters(flush_after=True)
        weights = self.get_weights()
        return pickle.dumps({
            "filters": filters,
            "weights": weights})

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


RemoteA3CEvaluator = ray.remote(A3CEvaluator)
GPURemoteA3CEvaluator = ray.remote(num_gpus=1)(A3CEvaluator)

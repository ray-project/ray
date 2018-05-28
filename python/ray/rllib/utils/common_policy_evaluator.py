import ray

import pickle
import tensorflow as tf

from ray.rllib.models import ModelCatalog
from ray.rllib.optimizers.policy_evaluator import PolicyEvaluator
from ray.rllib.utils.filter import get_filter
from ray.rllib.utils.sampler import AsyncSampler, SyncSampler
from ray.rllib.utils.tf_policy import TFPolicy
from ray.tune.registry import get_registry


class CommonPolicyEvaluator(PolicyEvaluator):
    """Policy evaluator implementation that operates on a rllib.Policy.

    TODO: vector env
    TODO: multi-agent
    TODO: consumer buffering
    TODO: complete episode batch mode
    """

    @classmethod
    def as_remote(cls, num_cpus=None, num_gpus=None):
        return ray.remote(num_cpus=num_cpus, num_gpus=num_gpus)(cls)

    def __init__(
            self, env_creator, policy_cls, tf_session_creator=None,
            min_batch_steps=100, batch_mode="complete_episodes",
            sample_async=False, compress_observations=False,
            consumer_buffer_size=0, observation_filter="NoFilter",
            registry=None, env_config=None, model_config=None,
            policy_config=None):
        """Initialize a policy evaluator.

        Arguments:
            env_creator (func): Function that returns a gym.Env given an
                env config dict.
            policy_cls (class): A function that returns an
                object implementing rllib.Policy or rllib.TFPolicy.
            tf_session_creator (func): A function that returns a TF session.
                This is optional and only useful with TFPolicy.
            min_batch_steps (int): The minimum number of env steps to include
                in each sample batch returned from this evaluator.
            batch_mode (str): One of "complete_episodes", "truncate_episodes".
                This determines whether episodes are cut during sampling.
            sample_async (bool): Whether to compute samples asynchronously in
                the background, which improves throughput but can cause samples
                to be slightly off-policy.
            compress_observations (bool): If true, compress the observations
                returned.
            consumer_buffer_size (int): If non-zero, buffers up to N sample
                batches in-memory for use so that they can be retrieved
                by multiple consumers. This only makes sense in multi-agent.
        """

        registry = registry or get_registry()
        env_config = env_config or {}
        policy_config = policy_config or {}
        model_config = model_config or {}

        assert batch_mode in ["complete_episodes", "truncate_episodes"]
        self.env_creator = env_creator
        self.policy_cls = policy_cls
        self.min_batch_steps = min_batch_steps
        self.batch_mode = batch_mode
        self.env = ModelCatalog.get_preprocessor_as_wrapper(
            registry, env_creator(env_config), model_config)

        self.vectorized = hasattr(self.env, "vector_reset")
        self.policy_map = {}

        if issubclass(policy_cls, TFPolicy):
            with tf.Graph().as_default():
                if tf_session_creator:
                    sess = tf_session_creator()
                else:
                    sess = tf.Session(config=tf.ConfigProto(
                        gpu_options=tf.GPUOptions(allow_growth=True)))
                with sess.as_default():
                    policy = policy_cls(
                        registry, self.env.observation_space,
                        self.env.action_space, policy_config)
        else:
            policy = policy_cls(
                registry, self.env.observation_space, self.env.action_space,
                policy_config)
        self.policy_map = {
            "default": policy
        }

        self.obs_filter = get_filter(
            observation_filter, self.env.observation_space.shape)
        self.filters = {"obs_filter": self.obs_filter}

        if self.vectorized:
            raise NotImplementedError("Vector envs not yet supported")
        else:
            if batch_mode != "truncate_episodes":
                raise NotImplementedError("Batch mode not yet supported")
            if sample_async:
                self.sampler = AsyncSampler(
                    self.env, self.policy_map["default"], self.obs_filter,
                    min_batch_steps)
                self.sampler.start()
            else:
                self.sampler = SyncSampler(
                    self.env, self.policy_map["default"], self.obs_filter,
                    min_batch_steps)

        if compress_observations:
            raise NotImplementedError("Sample compression not yet supported")

        if consumer_buffer_size:
            raise NotImplementedError("Sample buffering not yet supported")

    def sample(self, consumer_uuid=None):
        """Evaluate the current policies and return a batch of experiences.

        Arguments:
            consumer_uuid (str): Unique id for the consumer. This enables the
                sharing of experiences across multiple consumers.

        Return:
            SampleBatch from evaluating the current policies.
        """

        return self.policy_map["default"].postprocess_trajectory(
            self.sampler.get_data())

    def apply(self, func):
        """Apply the given function to this evaluator instance."""

        return func(self)

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

    def get_weights(self):
        return self.policy_map["default"].get_weights()

    def set_weights(self, weights):
        return self.policy_map["default"].set_weights(weights)

    def compute_gradients(self, samples):
        return self.policy_map["default"].compute_gradients(samples)

    def apply_gradients(self, grads):
        return self.policy_map["default"].apply_gradients(grads)

    def save(self):
        filters = self.get_filters(flush_after=True)
        weights = self.policy_map["default"].get_weights()
        return pickle.dumps({"filters": filters, "weights": weights})

    def restore(self, objs):
        objs = pickle.loads(objs)
        self.sync_filters(objs["filters"])
        self.policy_map["default"].set_weights(objs["weights"])

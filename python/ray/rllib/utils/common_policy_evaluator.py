from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import pickle
import numpy as np
import tensorflow as tf

import ray
from ray.rllib.models import ModelCatalog
from ray.rllib.optimizers.policy_evaluator import PolicyEvaluator
from ray.rllib.utils.atari_wrappers import wrap_deepmind
from ray.rllib.utils.compression import pack
from ray.rllib.utils.filter import get_filter
from ray.rllib.utils.sampler import AsyncSampler, SyncSampler
from ray.rllib.utils.tf_policy_graph import TFPolicyGraph
from ray.tune.registry import get_registry
from ray.tune.result import TrainingResult


def collect_metrics(local_evaluator, remote_evaluators):
    """Gathers episode metrics from CommonPolicyEvaluator instances."""

    episode_rewards = []
    episode_lengths = []
    metric_lists = ray.get(
        [a.apply.remote(lambda ev: ev.sampler.get_metrics())
         for a in remote_evaluators])
    metric_lists.append(local_evaluator.sampler.get_metrics())
    for metrics in metric_lists:
        for episode in metrics:
            episode_lengths.append(episode.episode_length)
            episode_rewards.append(episode.episode_reward)
    if episode_rewards:
        min_reward = min(episode_rewards)
        max_reward = max(episode_rewards)
    else:
        min_reward = float('nan')
        max_reward = float('nan')
    avg_reward = np.mean(episode_rewards)
    avg_length = np.mean(episode_lengths)
    timesteps = np.sum(episode_lengths)

    return TrainingResult(
        episode_reward_max=max_reward,
        episode_reward_min=min_reward,
        episode_reward_mean=avg_reward,
        episode_len_mean=avg_length,
        episodes_total=len(episode_lengths),
        timesteps_this_iter=timesteps)


class CommonPolicyEvaluator(PolicyEvaluator):
    """Policy evaluator implementation that operates on a rllib.PolicyGraph.

    TODO: vector env
    TODO: multi-agent
    TODO: consumer buffering for multi-agent
    TODO: complete episode batch mode

    Examples:
        # Create a policy evaluator and using it to collect experiences.
        >>> evaluator = CommonPolicyEvaluator(
              env_creator=lambda _: gym.make("CartPole-v0"),
              policy_graph=PGPolicyGraph)
        >>> print(evaluator.sample().keys())
        {"obs": [[...]], "actions": [[...]], "rewards": [[...]],
         "dones": [[...]], "new_obs": [[...]]}

        # Creating policy evaluators using optimizer_cls.make().
        >>> optimizer = LocalSyncOptimizer.make(
              evaluator_cls=CommonPolicyEvaluator,
              evaluator_args={
                "env_creator": lambda _: gym.make("CartPole-v0"),
                "policy_graph": PGPolicyGraph,
              },
              num_workers=10)
        >>> for _ in range(10): optimizer.step()
    """

    @classmethod
    def as_remote(cls, num_cpus=None, num_gpus=None):
        return ray.remote(num_cpus=num_cpus, num_gpus=num_gpus)(cls)

    def __init__(
            self,
            env_creator,
            policy_graph,
            tf_session_creator=None,
            batch_steps=100,
            batch_mode="truncate_episodes",
            preprocessor_pref="rllib",
            sample_async=False,
            compress_observations=False,
            observation_filter="NoFilter",
            registry=None,
            env_config=None,
            model_config=None,
            policy_config=None):
        """Initialize a policy evaluator.

        Arguments:
            env_creator (func): Function that returns a gym.Env given an
                env config dict.
            policy_graph (class): A class implementing rllib.PolicyGraph or
                rllib.TFPolicyGraph.
            tf_session_creator (func): A function that returns a TF session.
                This is optional and only useful with TFPolicyGraph.
            batch_steps (int): The target number of env transitions to include
                in each sample batch returned from this evaluator.
            batch_mode (str): One of the following choices:
                complete_episodes: each batch will be at least batch_steps
                    in size, and will include one or more complete episodes.
                truncate_episodes: each batch will be around batch_steps
                    in size, and include transitions from one episode only.
                pack_episodes: each batch will be exactly batch_steps in
                    size, and may include transitions from multiple episodes.
            preprocessor_pref (str): Whether to prefer RLlib preprocessors
                ("rllib") or deepmind ("deepmind") when applicable.
            sample_async (bool): Whether to compute samples asynchronously in
                the background, which improves throughput but can cause samples
                to be slightly off-policy.
            compress_observations (bool): If true, compress the observations
                returned.
            observation_filter (str): Name of observation filter to use.
            registry (tune.Registry): User-registered objects. Pass in the
                value from tune.registry.get_registry() if you're having
                trouble resolving things like custom envs.
            env_config (dict): Config to pass to the env creator.
            model_config (dict): Config to use when creating the policy model.
            policy_config (dict): Config to pass to the policy.
        """

        registry = registry or get_registry()
        env_config = env_config or {}
        policy_config = policy_config or {}
        model_config = model_config or {}

        assert batch_mode in [
            "complete_episodes", "truncate_episodes", "pack_episodes"]
        self.env_creator = env_creator
        self.policy_graph = policy_graph
        self.batch_steps = batch_steps
        self.batch_mode = batch_mode
        self.compress_observations = compress_observations

        self.env = env_creator(env_config)
        is_atari = hasattr(self.env.unwrapped, "ale")
        if is_atari and "custom_preprocessor" not in model_config and \
                preprocessor_pref == "deepmind":
            self.env = wrap_deepmind(self.env, dim=model_config.get("dim", 80))
        else:
            self.env = ModelCatalog.get_preprocessor_as_wrapper(
                registry, self.env, model_config)

        self.vectorized = hasattr(self.env, "vector_reset")
        self.policy_map = {}

        if issubclass(policy_graph, TFPolicyGraph):
            with tf.Graph().as_default():
                if tf_session_creator:
                    self.sess = tf_session_creator()
                else:
                    self.sess = tf.Session(config=tf.ConfigProto(
                        gpu_options=tf.GPUOptions(allow_growth=True)))
                with self.sess.as_default():
                    policy = policy_graph(
                        self.env.observation_space, self.env.action_space,
                        registry, policy_config)
        else:
            policy = policy_graph(
                self.env.observation_space, self.env.action_space,
                registry, policy_config)
        self.policy_map = {
            "default": policy
        }

        self.obs_filter = get_filter(
            observation_filter, self.env.observation_space.shape)
        self.filters = {"obs_filter": self.obs_filter}

        if self.vectorized:
            raise NotImplementedError("Vector envs not yet supported")
        else:
            if batch_mode not in [
                    "pack_episodes", "truncate_episodes", "complete_episodes"]:
                raise NotImplementedError("Batch mode not yet supported")
            pack = batch_mode == "pack_episodes"
            if batch_mode == "complete_episodes":
                batch_steps = 999999
            if sample_async:
                self.sampler = AsyncSampler(
                    self.env, self.policy_map["default"], self.obs_filter,
                    batch_steps, pack=pack)
                self.sampler.start()
            else:
                self.sampler = SyncSampler(
                    self.env, self.policy_map["default"], self.obs_filter,
                    batch_steps, pack=pack)

    def sample(self):
        """Evaluate the current policies and return a batch of experiences.

        Return:
            SampleBatch from evaluating the current policies.
        """

        batch = self.policy_map["default"].postprocess_trajectory(
            self.sampler.get_data())

        if self.compress_observations:
            batch["obs"] = [pack(o) for o in batch["obs"]]
            batch["new_obs"] = [pack(o) for o in batch["new_obs"]]

        return batch

    def apply(self, func):
        """Apply the given function to this evaluator instance."""

        return func(self)

    def for_policy(self, func):
        """Apply the given function to this evaluator's default policy."""

        return func(self.policy_map["default"])

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

    def compute_apply(self, samples):
        grad_fetch, apply_fetch = self.policy_map["default"].compute_apply(
            samples)
        return grad_fetch

    def save(self):
        filters = self.get_filters(flush_after=True)
        state = self.policy_map["default"].get_state()
        return pickle.dumps({"filters": filters, "state": state})

    def restore(self, objs):
        objs = pickle.loads(objs)
        self.sync_filters(objs["filters"])
        self.policy_map["default"].set_state(objs["state"])

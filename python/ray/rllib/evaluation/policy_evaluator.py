from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import gym
import pickle
import tensorflow as tf

import ray
from ray.rllib.models import ModelCatalog
from ray.rllib.env.async_vector_env import AsyncVectorEnv
from ray.rllib.env.atari_wrappers import wrap_deepmind, is_atari
from ray.rllib.env.env_context import EnvContext
from ray.rllib.env.serving_env import ServingEnv
from ray.rllib.env.vector_env import VectorEnv
from ray.rllib.env.multi_agent_env import MultiAgentEnv
from ray.rllib.evaluation.interface import EvaluatorInterface
from ray.rllib.evaluation.sample_batch import MultiAgentBatch, \
    DEFAULT_POLICY_ID
from ray.rllib.evaluation.sampler import AsyncSampler, SyncSampler
from ray.rllib.utils.compression import pack
from ray.rllib.utils.filter import get_filter
from ray.rllib.evaluation.policy_graph import PolicyGraph
from ray.rllib.evaluation.tf_policy_graph import TFPolicyGraph
from ray.rllib.utils.tf_run_builder import TFRunBuilder


class PolicyEvaluator(EvaluatorInterface):
    """Common ``PolicyEvaluator`` implementation that wraps a ``PolicyGraph``.

    This class wraps a policy graph instance and an environment class to
    collect experiences from the environment. You can create many replicas of
    this class as Ray actors to scale RL training.

    This class supports vectorized and multi-agent policy evaluation (e.g.,
    VectorEnv, MultiAgentEnv, etc.)

    Examples:
        >>> # Create a policy evaluator and using it to collect experiences.
        >>> evaluator = PolicyEvaluator(
        ...   env_creator=lambda _: gym.make("CartPole-v0"),
        ...   policy_graph=PGPolicyGraph)
        >>> print(evaluator.sample())
        SampleBatch({
            "obs": [[...]], "actions": [[...]], "rewards": [[...]],
            "dones": [[...]], "new_obs": [[...]]})

        >>> # Creating policy evaluators using optimizer_cls.make().
        >>> optimizer = SyncSamplesOptimizer.make(
        ...   evaluator_cls=PolicyEvaluator,
        ...   evaluator_args={
        ...     "env_creator": lambda _: gym.make("CartPole-v0"),
        ...     "policy_graph": PGPolicyGraph,
        ...   },
        ...   num_workers=10)
        >>> for _ in range(10): optimizer.step()

        >>> # Creating a multi-agent policy evaluator
        >>> evaluator = PolicyEvaluator(
        ...   env_creator=lambda _: MultiAgentTrafficGrid(num_cars=25),
        ...   policy_graphs={
        ...       # Use an ensemble of two policies for car agents
        ...       "car_policy1":
        ...         (PGPolicyGraph, Box(...), Discrete(...), {"gamma": 0.99}),
        ...       "car_policy2":
        ...         (PGPolicyGraph, Box(...), Discrete(...), {"gamma": 0.95}),
        ...       # Use a single shared policy for all traffic lights
        ...       "traffic_light_policy":
        ...         (PGPolicyGraph, Box(...), Discrete(...), {}),
        ...   },
        ...   policy_mapping_fn=lambda agent_id:
        ...     random.choice(["car_policy1", "car_policy2"])
        ...     if agent_id.startswith("car_") else "traffic_light_policy")
        >>> print(evaluator.sample().keys())
        MultiAgentBatch({
            "car_policy1": SampleBatch(...),
            "car_policy2": SampleBatch(...),
            "traffic_light_policy": SampleBatch(...)})
    """

    @classmethod
    def as_remote(cls, num_cpus=None, num_gpus=None):
        return ray.remote(num_cpus=num_cpus, num_gpus=num_gpus)(cls)

    def __init__(self,
                 env_creator,
                 policy_graph,
                 policy_mapping_fn=None,
                 policies_to_train=None,
                 tf_session_creator=None,
                 batch_steps=100,
                 batch_mode="truncate_episodes",
                 episode_horizon=None,
                 preprocessor_pref="deepmind",
                 sample_async=False,
                 compress_observations=False,
                 num_envs=1,
                 observation_filter="NoFilter",
                 clip_rewards=False,
                 env_config=None,
                 model_config=None,
                 policy_config=None,
                 worker_index=0,
                 monitor_path=None):
        """Initialize a policy evaluator.

        Arguments:
            env_creator (func): Function that returns a gym.Env given an
                EnvContext wrapped configuration.
            policy_graph (class|dict): Either a class implementing
                PolicyGraph, or a dictionary of policy id strings to
                (PolicyGraph, obs_space, action_space, config) tuples. If a
                dict is specified, then we are in multi-agent mode and a
                policy_mapping_fn should also be set.
            policy_mapping_fn (func): A function that maps agent ids to
                policy ids in multi-agent mode. This function will be called
                each time a new agent appears in an episode, to bind that agent
                to a policy for the duration of the episode.
            policies_to_train (list): Optional whitelist of policies to train,
                or None for all policies.
            tf_session_creator (func): A function that returns a TF session.
                This is optional and only useful with TFPolicyGraph.
            batch_steps (int): The target number of env transitions to include
                in each sample batch returned from this evaluator.
            batch_mode (str): One of the following batch modes:
                "truncate_episodes": Each call to sample() will return a batch
                    of at most `batch_steps` in size. The batch will be exactly
                    `batch_steps` in size if postprocessing does not change
                    batch sizes. Episodes may be truncated in order to meet
                    this size requirement. When `num_envs > 1`, episodes will
                    be truncated to sequences of `batch_size / num_envs` in
                    length.
                "complete_episodes": Each call to sample() will return a batch
                    of at least `batch_steps in size. Episodes will not be
                    truncated, but multiple episodes may be packed within one
                    batch to meet the batch size. Note that when
                    `num_envs > 1`, episode steps will be buffered until the
                    episode completes, and hence batches may contain
                    significant amounts of off-policy data.
            episode_horizon (int): Whether to stop episodes at this horizon.
            preprocessor_pref (str): Whether to prefer RLlib preprocessors
                ("rllib") or deepmind ("deepmind") when applicable.
            sample_async (bool): Whether to compute samples asynchronously in
                the background, which improves throughput but can cause samples
                to be slightly off-policy.
            compress_observations (bool): If true, compress the observations.
                They can be decompressed with rllib/utils/compression.
            num_envs (int): If more than one, will create multiple envs
                and vectorize the computation of actions. This has no effect if
                if the env already implements VectorEnv.
            observation_filter (str): Name of observation filter to use.
            clip_rewards (bool): Whether to clip rewards to [-1, 1] prior to
                experience postprocessing.
            env_config (dict): Config to pass to the env creator.
            model_config (dict): Config to use when creating the policy model.
            policy_config (dict): Config to pass to the policy. In the
                multi-agent case, this config will be merged with the
                per-policy configs specified by `policy_graph`.
            worker_index (int): For remote evaluators, this should be set to a
                non-zero and unique value. This index is passed to created envs
                through EnvContext so that envs can be configured per worker.
            monitor_path (str): Write out episode stats and videos to this
                directory if specified.
        """

        env_context = EnvContext(env_config or {}, worker_index)
        policy_config = policy_config or {}
        self.policy_config = policy_config
        model_config = model_config or {}
        policy_mapping_fn = (policy_mapping_fn
                             or (lambda agent_id: DEFAULT_POLICY_ID))
        self.env_creator = env_creator
        self.batch_steps = batch_steps
        self.batch_mode = batch_mode
        self.compress_observations = compress_observations

        self.env = env_creator(env_context)
        if isinstance(self.env, VectorEnv) or \
                isinstance(self.env, ServingEnv) or \
                isinstance(self.env, MultiAgentEnv) or \
                isinstance(self.env, AsyncVectorEnv):

            def wrap(env):
                return env  # we can't auto-wrap these env types
        elif is_atari(self.env) and \
                "custom_preprocessor" not in model_config and \
                preprocessor_pref == "deepmind":

            def wrap(env):
                env = wrap_deepmind(
                    env,
                    dim=model_config.get("dim", 84),
                    framestack=not model_config.get("use_lstm")
                    and not model_config.get("no_framestack"))
                if monitor_path:
                    env = _monitor(env, monitor_path)
                return env
        else:

            def wrap(env):
                env = ModelCatalog.get_preprocessor_as_wrapper(
                    env, model_config)
                if monitor_path:
                    env = _monitor(env, monitor_path)
                return env

        self.env = wrap(self.env)

        def make_env(vector_index):
            return wrap(
                env_creator(env_context.with_vector_index(vector_index)))

        self.tf_sess = None
        policy_dict = _validate_and_canonicalize(policy_graph, self.env)
        self.policies_to_train = policies_to_train or list(policy_dict.keys())
        if _has_tensorflow_graph(policy_dict):
            with tf.Graph().as_default():
                if tf_session_creator:
                    self.tf_sess = tf_session_creator()
                else:
                    self.tf_sess = tf.Session(
                        config=tf.ConfigProto(
                            gpu_options=tf.GPUOptions(allow_growth=True)))
                with self.tf_sess.as_default():
                    self.policy_map = self._build_policy_map(
                        policy_dict, policy_config)
        else:
            self.policy_map = self._build_policy_map(policy_dict,
                                                     policy_config)

        self.multiagent = self.policy_map.keys() != set(DEFAULT_POLICY_ID)

        self.filters = {
            policy_id: get_filter(observation_filter,
                                  policy.observation_space.shape)
            for (policy_id, policy) in self.policy_map.items()
        }

        # Always use vector env for consistency even if num_envs = 1
        self.async_env = AsyncVectorEnv.wrap_async(
            self.env, make_env=make_env, num_envs=num_envs)
        self.num_envs = num_envs

        if self.batch_mode == "truncate_episodes":
            if batch_steps % num_envs != 0:
                raise ValueError(
                    "In 'truncate_episodes' batch mode, `batch_steps` must be "
                    "evenly divisible by `num_envs`. Got {} and {}.".format(
                        batch_steps, num_envs))
            batch_steps = batch_steps // num_envs
            pack_episodes = True
        elif self.batch_mode == "complete_episodes":
            batch_steps = float("inf")  # never cut episodes
            pack_episodes = False  # sampler will return 1 episode per poll
        else:
            raise ValueError("Unsupported batch mode: {}".format(
                self.batch_mode))
        if sample_async:
            self.sampler = AsyncSampler(
                self.async_env,
                self.policy_map,
                policy_mapping_fn,
                self.filters,
                clip_rewards,
                batch_steps,
                horizon=episode_horizon,
                pack=pack_episodes,
                tf_sess=self.tf_sess)
            self.sampler.start()
        else:
            self.sampler = SyncSampler(
                self.async_env,
                self.policy_map,
                policy_mapping_fn,
                self.filters,
                clip_rewards,
                batch_steps,
                horizon=episode_horizon,
                pack=pack_episodes,
                tf_sess=self.tf_sess)

    def _build_policy_map(self, policy_dict, policy_config):
        policy_map = {}
        for name, (cls, obs_space, act_space,
                   conf) in sorted(policy_dict.items()):
            merged_conf = policy_config.copy()
            merged_conf.update(conf)
            with tf.variable_scope(name):
                policy_map[name] = cls(obs_space, act_space, merged_conf)
        return policy_map

    def sample(self):
        """Evaluate the current policies and return a batch of experiences.

        Return:
            SampleBatch|MultiAgentBatch from evaluating the current policies.
        """

        batches = [self.sampler.get_data()]
        steps_so_far = batches[0].count

        # In truncate_episodes mode, never pull more than 1 batch per env.
        # This avoids over-running the target batch size.
        if self.batch_mode == "truncate_episodes":
            max_batches = self.num_envs
        else:
            max_batches = float("inf")

        while steps_so_far < self.batch_steps and len(batches) < max_batches:
            batch = self.sampler.get_data()
            steps_so_far += batch.count
            batches.append(batch)
        batches.extend(self.sampler.get_extra_batches())
        batch = batches[0].concat_samples(batches)

        if self.compress_observations:
            if isinstance(batch, MultiAgentBatch):
                for data in batch.policy_batches.values():
                    data["obs"] = [pack(o) for o in data["obs"]]
                    data["new_obs"] = [pack(o) for o in data["new_obs"]]
            else:
                batch["obs"] = [pack(o) for o in batch["obs"]]
                batch["new_obs"] = [pack(o) for o in batch["new_obs"]]

        return batch

    @ray.method(num_return_vals=2)
    def sample_with_count(self):
        """Same as sample() but returns the count as a separate future."""
        batch = self.sample()
        return batch, batch.count

    def for_policy(self, func, policy_id=DEFAULT_POLICY_ID):
        """Apply the given function to the specified policy graph."""

        return func(self.policy_map[policy_id])

    def foreach_policy(self, func):
        """Apply the given function to each (policy, policy_id) tuple."""

        return [func(policy, pid) for pid, policy in self.policy_map.items()]

    def foreach_trainable_policy(self, func):
        """Apply the given function to each (policy, policy_id) tuple.

        This only applies func to policies in `self.policies_to_train`."""

        return [
            func(policy, pid) for pid, policy in self.policy_map.items()
            if pid in self.policies_to_train
        ]

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

    def get_weights(self, policies=None):
        if policies is None:
            policies = self.policy_map.keys()
        return {
            pid: policy.get_weights()
            for pid, policy in self.policy_map.items() if pid in policies
        }

    def set_weights(self, weights):
        for pid, w in weights.items():
            self.policy_map[pid].set_weights(w)

    def compute_gradients(self, samples):
        if isinstance(samples, MultiAgentBatch):
            grad_out, info_out = {}, {}
            if self.tf_sess is not None:
                builder = TFRunBuilder(self.tf_sess, "compute_gradients")
                for pid, batch in samples.policy_batches.items():
                    if pid not in self.policies_to_train:
                        continue
                    grad_out[pid], info_out[pid] = (
                        self.policy_map[pid].build_compute_gradients(
                            builder, batch))
                grad_out = {k: builder.get(v) for k, v in grad_out.items()}
                info_out = {k: builder.get(v) for k, v in info_out.items()}
            else:
                for pid, batch in samples.policy_batches.items():
                    grad_out[pid], info_out[pid] = (
                        self.policy_map[pid].compute_gradients(batch))
        else:
            grad_out, info_out = (
                self.policy_map[DEFAULT_POLICY_ID].compute_gradients(samples))
        info_out["batch_count"] = samples.count
        return grad_out, info_out

    def apply_gradients(self, grads):
        if isinstance(grads, dict):
            if self.tf_sess is not None:
                builder = TFRunBuilder(self.tf_sess, "apply_gradients")
                outputs = {
                    pid: self.policy_map[pid].build_apply_gradients(
                        builder, grad)
                    for pid, grad in grads.items()
                }
                return {k: builder.get(v) for k, v in outputs.items()}
            else:
                return {
                    pid: self.policy_map[pid].apply_gradients(g)
                    for pid, g in grads.items()
                }
        else:
            return self.policy_map[DEFAULT_POLICY_ID].apply_gradients(grads)

    def compute_apply(self, samples):
        if isinstance(samples, MultiAgentBatch):
            info_out = {}
            if self.tf_sess is not None:
                builder = TFRunBuilder(self.tf_sess, "compute_apply")
                for pid, batch in samples.policy_batches.items():
                    if pid not in self.policies_to_train:
                        continue
                    info_out[pid], _ = (
                        self.policy_map[pid].build_compute_apply(
                            builder, batch))
                info_out = {k: builder.get(v) for k, v in info_out.items()}
            else:
                for pid, batch in samples.policy_batches.items():
                    info_out[pid], _ = (
                        self.policy_map[pid].compute_apply(batch))
            return info_out
        else:
            grad_fetch, apply_fetch = (
                self.policy_map[DEFAULT_POLICY_ID].compute_apply(samples))
            return grad_fetch

    def save(self):
        filters = self.get_filters(flush_after=True)
        state = {
            pid: self.policy_map[pid].get_state()
            for pid in self.policy_map
        }
        return pickle.dumps({"filters": filters, "state": state})

    def restore(self, objs):
        objs = pickle.loads(objs)
        self.sync_filters(objs["filters"])
        for pid, state in objs["state"].items():
            self.policy_map[pid].set_state(state)

    def set_global_vars(self, global_vars):
        self.foreach_policy(lambda p, _: p.on_global_var_update(global_vars))


def _validate_and_canonicalize(policy_graph, env):
    if isinstance(policy_graph, dict):
        for k, v in policy_graph.items():
            if not isinstance(k, str):
                raise ValueError(
                    "policy_graph keys must be strs, got {}".format(type(k)))
            if not isinstance(v, tuple) or len(v) != 4:
                raise ValueError(
                    "policy_graph values must be tuples of "
                    "(cls, obs_space, action_space, config), got {}".format(v))
            if not issubclass(v[0], PolicyGraph):
                raise ValueError(
                    "policy_graph tuple value 0 must be a rllib.PolicyGraph "
                    "class, got {}".format(v[0]))
            if not isinstance(v[1], gym.Space):
                raise ValueError(
                    "policy_graph tuple value 1 (observation_space) must be a "
                    "gym.Space, got {}".format(type(v[1])))
            if not isinstance(v[2], gym.Space):
                raise ValueError(
                    "policy_graph tuple value 2 (action_space) must be a "
                    "gym.Space, got {}".format(type(v[2])))
            if not isinstance(v[3], dict):
                raise ValueError(
                    "policy_graph tuple value 3 (config) must be a dict, "
                    "got {}".format(type(v[3])))
        return policy_graph
    elif not issubclass(policy_graph, PolicyGraph):
        raise ValueError("policy_graph must be a rllib.PolicyGraph class")
    else:
        return {
            DEFAULT_POLICY_ID: (policy_graph, env.observation_space,
                                env.action_space, {})
        }


def _monitor(env, path):
    return gym.wrappers.Monitor(env, path, resume=True)


def _has_tensorflow_graph(policy_dict):
    for policy, _, _, _ in policy_dict.values():
        if issubclass(policy, TFPolicyGraph):
            return True
    return False

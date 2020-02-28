from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import tensorflow as tf

import ray
from ray.rllib.env.atari_wrappers import wrap_deepmind, is_atari
from ray.rllib.env.base_env import BaseEnv
from ray.rllib.env.env_context import EnvContext
from ray.rllib.env.multi_agent_env import MultiAgentEnv
from ray.rllib.env.external_multi_agent_env import ExternalMultiAgentEnv
from ray.rllib.evaluation.policy_evaluator import (
    PolicyEvaluator as UnpatchedPolicyEvaluator, _has_tensorflow_graph,
    _monitor, _validate_and_canonicalize, _validate_env,
    _validate_multiagent_config
)
from ray.rllib.evaluation.sample_batch import DEFAULT_POLICY_ID
from ray.rllib.offline import NoopOutput, IOContext, OutputWriter, InputReader
from ray.rllib.offline.is_estimator import ImportanceSamplingEstimator
from ray.rllib.offline.wis_estimator import WeightedImportanceSamplingEstimator
from ray.rllib.utils.annotations import DeveloperAPI
from ray.rllib.utils.debug import (
    disable_log_once_globally, enable_periodic_logging)
from ray.rllib.utils.filter import get_filter
from ray.rllib.agents.sac.sac.rllib_proxy._patched._sampler import AsyncSampler, SyncSampler


logger = logging.getLogger(__name__)

# Handle to the current evaluator, which will be set to the most recently
# created PolicyEvaluator in this process. This can be helpful to access in
# custom env or policy classes for debugging or advanced use cases.
_global_evaluator = None


@DeveloperAPI
def get_global_evaluator():
    """Returns a handle to the active policy evaluator in this process."""

    global _global_evaluator
    return _global_evaluator


@DeveloperAPI
class PolicyEvaluator(UnpatchedPolicyEvaluator):

    @DeveloperAPI
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
                 clip_rewards=None,
                 clip_actions=True,
                 env_config=None,
                 model_config=None,
                 policy_config=None,
                 worker_index=0,
                 monitor_path=None,
                 log_dir=None,
                 log_level=None,
                 callbacks=None,
                 input_creator=lambda ioctx: ioctx.default_sampler_input(),
                 input_evaluation=frozenset([]),
                 output_creator=lambda ioctx: NoopOutput(),
                 remote_worker_envs=False,
                 remote_env_batch_wait_ms=0,
                 soft_horizon=False,
                 no_done_at_end=False,
                 _fake_sampler=False):
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
                    of at most `batch_steps * num_envs` in size. The batch will
                    be exactly `batch_steps * num_envs` in size if
                    postprocessing does not change batch sizes. Episodes may be
                    truncated in order to meet this size requirement.
                "complete_episodes": Each call to sample() will return a batch
                    of at least `batch_steps * num_envs` in size. Episodes will
                    not be truncated, but multiple episodes may be packed
                    within one batch to meet the batch size. Note that when
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
                experience postprocessing. Setting to None means clip for Atari
                only.
            clip_actions (bool): Whether to clip action values to the range
                specified by the policy action space.
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
            log_dir (str): Directory where logs can be placed.
            log_level (str): Set the root log level on creation.
            callbacks (dict): Dict of custom debug callbacks.
            input_creator (func): Function that returns an InputReader object
                for loading previous generated experiences.
            input_evaluation (list): How to evaluate the policy performance.
                This only makes sense to set when the input is reading offline
                data. The possible values include:
                  - "is": the step-wise importance sampling estimator.
                  - "wis": the weighted step-wise is estimator.
                  - "simulation": run the environment in the background, but
                    use this data for evaluation only and never for learning.
            output_creator (func): Function that returns an OutputWriter object
                for saving generated experiences.
            remote_worker_envs (bool): If using num_envs > 1, whether to create
                those new envs in remote processes instead of in the current
                process. This adds overheads, but can make sense if your envs
            remote_env_batch_wait_ms (float): Timeout that remote workers
                are waiting when polling environments. 0 (continue when at
                least one env is ready) is a reasonable default, but optimal
                value could be obtained by measuring your environment
                step / reset and model inference perf.
            soft_horizon (bool): Calculate rewards but don't reset the
                environment when the horizon is hit.
            _fake_sampler (bool): Use a fake (inf speed) sampler for testing.
        """

        global _global_evaluator
        _global_evaluator = self

        if log_level:
            logging.getLogger("ray.rllib").setLevel(log_level)

        if worker_index > 1:
            disable_log_once_globally()  # only need 1 evaluator to log
        elif log_level == "DEBUG":
            enable_periodic_logging()

        env_context = EnvContext(env_config or {}, worker_index)
        policy_config = policy_config or {}
        self.policy_config = policy_config
        self.callbacks = callbacks or {}
        self.worker_index = worker_index
        model_config = model_config or {}
        policy_mapping_fn = (policy_mapping_fn
                             or (lambda agent_id: DEFAULT_POLICY_ID))
        if not callable(policy_mapping_fn):
            raise ValueError(
                "Policy mapping function not callable. If you're using Tune, "
                "make sure to escape the function with tune.function() "
                "to prevent it from being evaluated as an expression.")
        self.env_creator = env_creator
        self.sample_batch_size = batch_steps * num_envs
        self.batch_mode = batch_mode
        self.compress_observations = compress_observations
        self.preprocessing_enabled = True
        self.last_batch = None
        self._fake_sampler = _fake_sampler

        self.env = _validate_env(env_creator(env_context))
        if isinstance(self.env, MultiAgentEnv) or \
                isinstance(self.env, BaseEnv):

            def wrap(env):
                return env  # we can't auto-wrap these env types
        elif is_atari(self.env) and \
                not model_config.get("custom_preprocessor") and \
                preprocessor_pref == "deepmind":

            # Deepmind wrappers already handle all preprocessing
            self.preprocessing_enabled = False

            if clip_rewards is None:
                clip_rewards = True

            def wrap(env):
                env = wrap_deepmind(
                    env,
                    dim=model_config.get("dim"),
                    framestack=model_config.get("framestack"))
                if monitor_path:
                    env = _monitor(env, monitor_path)
                return env
        else:

            def wrap(env):
                if monitor_path:
                    env = _monitor(env, monitor_path)
                return env

        self.env = wrap(self.env)

        def make_env(vector_index):
            return wrap(
                env_creator(
                    env_context.copy_with_overrides(
                        vector_index=vector_index, remote=remote_worker_envs)))

        self.tf_sess = None
        policy_dict = _validate_and_canonicalize(policy_graph, self.env)
        self.policies_to_train = policies_to_train or list(policy_dict.keys())
        if _has_tensorflow_graph(policy_dict):
            if (ray.is_initialized()
                    and ray.worker._mode() != ray.worker.LOCAL_MODE
                    and not ray.get_gpu_ids()):
                logger.info("Creating policy evaluation worker {}".format(
                    worker_index) +
                            " on CPU (please ignore any CUDA init errors)")
            with tf.Graph().as_default():
                if tf_session_creator:
                    self.tf_sess = tf_session_creator()
                else:
                    self.tf_sess = tf.Session(
                        config=tf.ConfigProto(
                            gpu_options=tf.GPUOptions(allow_growth=True)))
                with self.tf_sess.as_default():
                    self.policy_map, self.preprocessors = \
                        self._build_policy_map(policy_dict, policy_config)
        else:
            self.policy_map, self.preprocessors = self._build_policy_map(
                policy_dict, policy_config)

        self.multiagent = set(self.policy_map.keys()) != {DEFAULT_POLICY_ID}
        if self.multiagent:
            if not ((isinstance(self.env, MultiAgentEnv)
                     or isinstance(self.env, ExternalMultiAgentEnv))
                    or isinstance(self.env, BaseEnv)):
                raise ValueError(
                    "Have multiple policy graphs {}, but the env ".format(
                        self.policy_map) +
                    "{} is not a subclass of BaseEnv, MultiAgentEnv or "
                    "ExternalMultiAgentEnv?".format(self.env))

        self.filters = {
            policy_id: get_filter(observation_filter,
                                  policy.observation_space.shape)
            for (policy_id, policy) in self.policy_map.items()
        }
        if self.worker_index == 0:
            logger.info("Built filter map: {}".format(self.filters))

        # Always use vector env for consistency even if num_envs = 1
        self.async_env = BaseEnv.to_base_env(
            self.env,
            make_env=make_env,
            num_envs=num_envs,
            remote_envs=remote_worker_envs,
            remote_env_batch_wait_ms=remote_env_batch_wait_ms)
        self.num_envs = num_envs

        if self.batch_mode == "truncate_episodes":
            unroll_length = batch_steps
            pack_episodes = True
        elif self.batch_mode == "complete_episodes":
            unroll_length = float("inf")  # never cut episodes
            pack_episodes = False  # sampler will return 1 episode per poll
        else:
            raise ValueError("Unsupported batch mode: {}".format(
                self.batch_mode))

        self.io_context = IOContext(log_dir, policy_config, worker_index, self)
        self.reward_estimators = []
        for method in input_evaluation:
            if method == "simulation":
                logger.warning(
                    "Requested 'simulation' input evaluation method: "
                    "will discard all sampler outputs and keep only metrics.")
                sample_async = True
            elif method == "is":
                ise = ImportanceSamplingEstimator.create(self.io_context)
                self.reward_estimators.append(ise)
            elif method == "wis":
                wise = WeightedImportanceSamplingEstimator.create(
                    self.io_context)
                self.reward_estimators.append(wise)
            else:
                raise ValueError(
                    "Unknown evaluation method: {}".format(method))

        if sample_async:
            self.sampler = AsyncSampler(
                self.async_env,
                self.policy_map,
                policy_mapping_fn,
                self.preprocessors,
                self.filters,
                clip_rewards,
                unroll_length,
                self.callbacks,
                horizon=episode_horizon,
                pack=pack_episodes,
                tf_sess=self.tf_sess,
                clip_actions=clip_actions,
                blackhole_outputs="simulation" in input_evaluation,
                soft_horizon=soft_horizon,
                no_done_at_end=no_done_at_end)
            self.sampler.start()
        else:
            self.sampler = SyncSampler(
                self.async_env,
                self.policy_map,
                policy_mapping_fn,
                self.preprocessors,
                self.filters,
                clip_rewards,
                unroll_length,
                self.callbacks,
                horizon=episode_horizon,
                pack=pack_episodes,
                tf_sess=self.tf_sess,
                clip_actions=clip_actions,
                soft_horizon=soft_horizon,
                no_done_at_end=no_done_at_end)

        self.input_reader = input_creator(self.io_context)
        assert isinstance(self.input_reader, InputReader), self.input_reader
        self.output_writer = output_creator(self.io_context)
        assert isinstance(self.output_writer, OutputWriter), self.output_writer

        logger.debug("Created evaluator with env {} ({}), policies {}".format(
            self.async_env, self.env, self.policy_map))

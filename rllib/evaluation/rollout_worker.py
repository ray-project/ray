import copy
import gym
import logging
import numpy as np
import platform
import os
from typing import Any, Callable, Dict, List, Optional, Tuple, Type, TypeVar, \
    TYPE_CHECKING, Union

import ray
from ray import cloudpickle as pickle
from ray.rllib.env.base_env import BaseEnv
from ray.rllib.env.env_context import EnvContext
from ray.rllib.env.external_env import ExternalEnv
from ray.rllib.env.multi_agent_env import MultiAgentEnv
from ray.rllib.env.external_multi_agent_env import ExternalMultiAgentEnv
from ray.rllib.env.utils import record_env_wrapper
from ray.rllib.env.vector_env import VectorEnv
from ray.rllib.env.wrappers.atari_wrappers import wrap_deepmind, is_atari
from ray.rllib.evaluation.sampler import AsyncSampler, SyncSampler
from ray.rllib.evaluation.rollout_metrics import RolloutMetrics
from ray.rllib.models import ModelCatalog
from ray.rllib.models.preprocessors import NoPreprocessor, Preprocessor
from ray.rllib.offline import NoopOutput, IOContext, OutputWriter, InputReader
from ray.rllib.offline.off_policy_estimator import OffPolicyEstimator, \
    OffPolicyEstimate
from ray.rllib.offline.is_estimator import ImportanceSamplingEstimator
from ray.rllib.offline.wis_estimator import WeightedImportanceSamplingEstimator
from ray.rllib.policy.sample_batch import MultiAgentBatch, DEFAULT_POLICY_ID
from ray.rllib.policy.policy import Policy, PolicySpec
from ray.rllib.policy.policy_map import PolicyMap
from ray.rllib.policy.torch_policy import TorchPolicy
from ray.rllib.utils import force_list, merge_dicts
from ray.rllib.utils.annotations import DeveloperAPI
from ray.rllib.utils.debug import summarize, update_global_seed_if_necessary
from ray.rllib.utils.deprecation import deprecation_warning
from ray.rllib.utils.error import EnvError, ERR_MSG_NO_GPUS, \
    HOWTO_CHANGE_CONFIG
from ray.rllib.utils.filter import get_filter, Filter
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.sgd import do_minibatch_sgd
from ray.rllib.utils.tf_ops import get_gpu_devices as get_tf_gpu_devices
from ray.rllib.utils.tf_run_builder import TFRunBuilder
from ray.rllib.utils.typing import AgentID, EnvConfigDict, EnvType, \
    ModelConfigDict, ModelGradients, ModelWeights, \
    MultiAgentPolicyConfigDict, PartialTrainerConfigDict, PolicyID, \
    SampleBatchType, TrainerConfigDict
from ray.util.debug import log_once, disable_log_once_globally, \
    enable_periodic_logging
from ray.util.iter import ParallelIteratorWorker

if TYPE_CHECKING:
    from ray.rllib.evaluation.episode import MultiAgentEpisode
    from ray.rllib.evaluation.observation_function import ObservationFunction
    from ray.rllib.agents.callbacks import DefaultCallbacks  # noqa

# Generic type var for foreach_* methods.
T = TypeVar("T")

tf1, tf, tfv = try_import_tf()
torch, _ = try_import_torch()

logger = logging.getLogger(__name__)

# Handle to the current rollout worker, which will be set to the most recently
# created RolloutWorker in this process. This can be helpful to access in
# custom env or policy classes for debugging or advanced use cases.
_global_worker: "RolloutWorker" = None


@DeveloperAPI
def get_global_worker() -> "RolloutWorker":
    """Returns a handle to the active rollout worker in this process."""

    global _global_worker
    return _global_worker


def _update_env_seed_if_necessary(env: EnvType, seed: int, worker_idx: int,
                                  vector_idx: int):
    """Set a deterministic random seed on environment.

    NOTE: this may not work with remote environments (issue #18154).
    """
    if not seed:
        return

    # A single RL job is unlikely to have more than 10K
    # rollout workers.
    max_num_envs_per_workers: int = 1000
    assert worker_idx < max_num_envs_per_workers, \
        "Too many envs per worker. Random seeds may collide."
    computed_seed: int = (
        worker_idx * max_num_envs_per_workers + vector_idx + seed)

    # Gym.env.
    # This will silently fail for most OpenAI gyms
    # (they do nothing and return None per default)
    if not hasattr(env, "seed"):
        logger.info("Env doesn't support env.seed(): {}".format(env))
    else:
        env.seed(computed_seed)


@DeveloperAPI
class RolloutWorker(ParallelIteratorWorker):
    """Common experience collection class.

    This class wraps a policy instance and an environment class to
    collect experiences from the environment. You can create many replicas of
    this class as Ray actors to scale RL training.

    This class supports vectorized and multi-agent policy evaluation (e.g.,
    VectorEnv, MultiAgentEnv, etc.)

    Examples:
        >>> # Create a rollout worker and using it to collect experiences.
        >>> worker = RolloutWorker(
        ...   env_creator=lambda _: gym.make("CartPole-v0"),
        ...   policy_spec=PGTFPolicy)
        >>> print(worker.sample())
        SampleBatch({
            "obs": [[...]], "actions": [[...]], "rewards": [[...]],
            "dones": [[...]], "new_obs": [[...]]})

        >>> # Creating a multi-agent rollout worker
        >>> worker = RolloutWorker(
        ...   env_creator=lambda _: MultiAgentTrafficGrid(num_cars=25),
        ...   policy_spec={
        ...       # Use an ensemble of two policies for car agents
        ...       "car_policy1":
        ...         (PGTFPolicy, Box(...), Discrete(...), {"gamma": 0.99}),
        ...       "car_policy2":
        ...         (PGTFPolicy, Box(...), Discrete(...), {"gamma": 0.95}),
        ...       # Use a single shared policy for all traffic lights
        ...       "traffic_light_policy":
        ...         (PGTFPolicy, Box(...), Discrete(...), {}),
        ...   },
        ...   policy_mapping_fn=lambda agent_id, episode, **kwargs:
        ...     random.choice(["car_policy1", "car_policy2"])
        ...     if agent_id.startswith("car_") else "traffic_light_policy")
        >>> print(worker.sample())
        MultiAgentBatch({
            "car_policy1": SampleBatch(...),
            "car_policy2": SampleBatch(...),
            "traffic_light_policy": SampleBatch(...)})
    """

    @DeveloperAPI
    @classmethod
    def as_remote(cls,
                  num_cpus: int = None,
                  num_gpus: int = None,
                  memory: int = None,
                  object_store_memory: int = None,
                  resources: dict = None) -> type:
        return ray.remote(
            num_cpus=num_cpus,
            num_gpus=num_gpus,
            memory=memory,
            object_store_memory=object_store_memory,
            resources=resources)(cls)

    @DeveloperAPI
    def __init__(
            self,
            *,
            env_creator: Callable[[EnvContext], EnvType],
            validate_env: Optional[Callable[[EnvType, EnvContext],
                                            None]] = None,
            policy_spec: Union[type, Dict[PolicyID, PolicySpec]] = None,
            policy_mapping_fn: Optional[Callable[
                [AgentID, "MultiAgentEpisode"], PolicyID]] = None,
            policies_to_train: Optional[List[PolicyID]] = None,
            tf_session_creator: Optional[Callable[[], "tf1.Session"]] = None,
            rollout_fragment_length: int = 100,
            count_steps_by: str = "env_steps",
            batch_mode: str = "truncate_episodes",
            episode_horizon: int = None,
            preprocessor_pref: Optional[str] = "deepmind",
            sample_async: bool = False,
            compress_observations: bool = False,
            num_envs: int = 1,
            observation_fn: "ObservationFunction" = None,
            observation_filter: str = "NoFilter",
            clip_rewards: Optional[Union[bool, float]] = None,
            normalize_actions: bool = True,
            clip_actions: bool = False,
            env_config: EnvConfigDict = None,
            model_config: ModelConfigDict = None,
            policy_config: TrainerConfigDict = None,
            worker_index: int = 0,
            num_workers: int = 0,
            record_env: Union[bool, str] = False,
            log_dir: str = None,
            log_level: str = None,
            callbacks: Type["DefaultCallbacks"] = None,
            input_creator: Callable[[
                IOContext
            ], InputReader] = lambda ioctx: ioctx.default_sampler_input(),
            input_evaluation: List[str] = frozenset([]),
            output_creator: Callable[
                [IOContext], OutputWriter] = lambda ioctx: NoopOutput(),
            remote_worker_envs: bool = False,
            remote_env_batch_wait_ms: int = 0,
            soft_horizon: bool = False,
            no_done_at_end: bool = False,
            seed: int = None,
            extra_python_environs: dict = None,
            fake_sampler: bool = False,
            spaces: Optional[Dict[PolicyID, Tuple[gym.spaces.Space,
                                                  gym.spaces.Space]]] = None,
            policy=None,
            monitor_path=None,
    ):
        """Initialize a rollout worker.

        Args:
            env_creator (Callable[[EnvContext], EnvType]): Function that
                returns a gym.Env given an EnvContext wrapped configuration.
            validate_env (Optional[Callable[[EnvType, EnvContext], None]]):
                Optional callable to validate the generated environment (only
                on worker=0).
            policy_spec (Optional[Union[Type[Policy],
                MultiAgentPolicyConfigDict]]): The MultiAgentPolicyConfigDict
                mapping policy IDs (str) to PolicySpec's or a single policy
                class to use.
                If a dict is specified, then we are in multi-agent mode and a
                policy_mapping_fn can also be set (if not, will map all agents
                to DEFAULT_POLICY_ID).
            policy_mapping_fn (Optional[Callable[[AgentID, MultiAgentEpisode],
                PolicyID]]): A callable that maps agent ids to policy ids in
                multi-agent mode. This function will be called each time a new
                agent appears in an episode, to bind that agent to a policy
                for the duration of the episode. If not provided, will map all
                agents to DEFAULT_POLICY_ID.
            policies_to_train (Optional[List[PolicyID]]): Optional list of
                policies to train, or None for all policies.
            tf_session_creator (Optional[Callable[[], tf1.Session]]): A
                function that returns a TF session. This is optional and only
                useful with TFPolicy.
            rollout_fragment_length (int): The target number of steps
                (maesured in `count_steps_by`) to include in each sample
                batch returned from this worker.
            count_steps_by (str): The unit in which to count fragment
                lengths. One of env_steps or agent_steps.
            batch_mode (str): One of the following batch modes:
                "truncate_episodes": Each call to sample() will return a batch
                    of at most `rollout_fragment_length * num_envs` in size.
                    The batch will be exactly
                    `rollout_fragment_length * num_envs` in size if
                    postprocessing does not change batch sizes. Episodes may be
                    truncated in order to meet this size requirement.
                "complete_episodes": Each call to sample() will return a batch
                    of at least `rollout_fragment_length * num_envs` in size.
                    Episodes will not be truncated, but multiple episodes may
                    be packed within one batch to meet the batch size. Note
                    that when `num_envs > 1`, episode steps will be buffered
                    until the episode completes, and hence batches may contain
                    significant amounts of off-policy data.
            episode_horizon (int): Whether to stop episodes at this horizon.
            preprocessor_pref (Optional[str]): Whether to use no preprocessor
                (None), RLlib preprocessors ("rllib") or deepmind ("deepmind"),
                when applicable.
            sample_async (bool): Whether to compute samples asynchronously in
                the background, which improves throughput but can cause samples
                to be slightly off-policy.
            compress_observations (bool): If true, compress the observations.
                They can be decompressed with rllib/utils/compression.
            num_envs (int): If more than one, will create multiple envs
                and vectorize the computation of actions. This has no effect if
                if the env already implements VectorEnv.
            observation_fn (ObservationFunction): Optional multi-agent
                observation function.
            observation_filter (str): Name of observation filter to use.
            clip_rewards (Optional[Union[bool, float]]): Whether to clip
                rewards to [-1.0, 1.0] prior to experience postprocessing.
                None: Clip for Atari only.
                float: Clip to [-clip_rewards; +clip_rewards].
            normalize_actions (bool): Whether to normalize actions to the
                action space's bounds.
            clip_actions (bool): Whether to clip action values to the range
                specified by the policy action space.
            env_config (EnvConfigDict): Config to pass to the env creator.
            model_config (ModelConfigDict): Config to use when creating the
                policy model.
            policy_config (TrainerConfigDict): Config to pass to the policy.
                In the multi-agent case, this config will be merged with the
                per-policy configs specified by `policy_spec`.
            worker_index (int): For remote workers, this should be set to a
                non-zero and unique value. This index is passed to created envs
                through EnvContext so that envs can be configured per worker.
            num_workers (int): For remote workers, how many workers altogether
                have been created?
            record_env (Union[bool, str]): Write out episode stats and videos
                using gym.wrappers.Monitor to this directory if specified. If
                True, use the default output dir in ~/ray_results/.... If
                False, do not record anything.
            log_dir (str): Directory where logs can be placed.
            log_level (str): Set the root log level on creation.
            callbacks (Type[DefaultCallbacks]): Custom sub-class of
                DefaultCallbacks for training/policy/rollout-worker callbacks.
            input_creator (Callable[[IOContext], InputReader]): Function that
                returns an InputReader object for loading previous generated
                experiences.
            input_evaluation (List[str]): How to evaluate the policy
                performance. This only makes sense to set when the input is
                reading offline data. The possible values include:
                  - "is": the step-wise importance sampling estimator.
                  - "wis": the weighted step-wise is estimator.
                  - "simulation": run the environment in the background, but
                    use this data for evaluation only and never for learning.
            output_creator (Callable[[IOContext], OutputWriter]): Function that
                returns an OutputWriter object for saving generated
                experiences.
            remote_worker_envs (bool): If using num_envs_per_worker > 1,
                whether to create those new envs in remote processes instead of
                in the current process. This adds overheads, but can make sense
                if your envs are expensive to step/reset (e.g., for StarCraft).
                Use this cautiously, overheads are significant!
            remote_env_batch_wait_ms (float): Timeout that remote workers
                are waiting when polling environments. 0 (continue when at
                least one env is ready) is a reasonable default, but optimal
                value could be obtained by measuring your environment
                step / reset and model inference perf.
            soft_horizon (bool): Calculate rewards but don't reset the
                environment when the horizon is hit.
            no_done_at_end (bool): Ignore the done=True at the end of the
                episode and instead record done=False.
            seed (int): Set the seed of both np and tf to this value to
                to ensure each remote worker has unique exploration behavior.
            extra_python_environs (dict): Extra python environments need to
                be set.
            fake_sampler (bool): Use a fake (inf speed) sampler for testing.
            spaces (Optional[Dict[PolicyID, Tuple[gym.spaces.Space,
                gym.spaces.Space]]]): An optional space dict mapping policy IDs
                to (obs_space, action_space)-tuples. This is used in case no
                Env is created on this RolloutWorker.
            policy: Obsoleted arg. Use `policy_spec` instead.
            monitor_path: Obsoleted arg. Use `record_env` instead.
        """

        # Deprecated args.
        if policy is not None:
            deprecation_warning("policy", "policy_spec", error=False)
            policy_spec = policy
        assert policy_spec is not None, \
            "Must provide `policy_spec` when creating RolloutWorker!"

        # Do quick translation into MultiAgentPolicyConfigDict.
        if not isinstance(policy_spec, dict):
            policy_spec = {
                DEFAULT_POLICY_ID: PolicySpec(policy_class=policy_spec)
            }
        policy_spec = {
            pid: spec if isinstance(spec, PolicySpec) else PolicySpec(*spec)
            for pid, spec in policy_spec.copy().items()
        }

        if monitor_path is not None:
            deprecation_warning("monitor_path", "record_env", error=False)
            record_env = monitor_path

        self._original_kwargs: dict = locals().copy()
        del self._original_kwargs["self"]

        global _global_worker
        _global_worker = self

        # set extra environs first
        if extra_python_environs:
            for key, value in extra_python_environs.items():
                os.environ[key] = str(value)

        def gen_rollouts():
            while True:
                yield self.sample()

        ParallelIteratorWorker.__init__(self, gen_rollouts, False)

        policy_config: TrainerConfigDict = policy_config or {}
        if (tf1 and policy_config.get("framework") in ["tf2", "tfe"]
                # This eager check is necessary for certain all-framework tests
                # that use tf's eager_mode() context generator.
                and not tf1.executing_eagerly()):
            tf1.enable_eager_execution()

        if log_level:
            logging.getLogger("ray.rllib").setLevel(log_level)

        if worker_index > 1:
            disable_log_once_globally()  # only need 1 worker to log
        elif log_level == "DEBUG":
            enable_periodic_logging()

        env_context = EnvContext(
            env_config or {},
            worker_index=worker_index,
            vector_index=0,
            num_workers=num_workers,
        )
        self.env_context = env_context
        self.policy_config: TrainerConfigDict = policy_config
        if callbacks:
            self.callbacks: "DefaultCallbacks" = callbacks()
        else:
            from ray.rllib.agents.callbacks import DefaultCallbacks  # noqa
            self.callbacks: DefaultCallbacks = DefaultCallbacks()
        self.worker_index: int = worker_index
        self.num_workers: int = num_workers
        model_config: ModelConfigDict = \
            model_config or self.policy_config.get("model") or {}

        # Default policy mapping fn is to always return DEFAULT_POLICY_ID,
        # independent on the agent ID and the episode passed in.
        self.policy_mapping_fn = \
            lambda agent_id, episode, worker, **kwargs: DEFAULT_POLICY_ID
        # If provided, set it here.
        self.set_policy_mapping_fn(policy_mapping_fn)

        self.env_creator: Callable[[EnvContext], EnvType] = env_creator
        self.rollout_fragment_length: int = rollout_fragment_length * num_envs
        self.count_steps_by: str = count_steps_by
        self.batch_mode: str = batch_mode
        self.compress_observations: bool = compress_observations
        self.preprocessing_enabled: bool = False \
            if preprocessor_pref is None else True
        self.observation_filter = observation_filter
        self.last_batch: SampleBatchType = None
        self.global_vars: dict = None
        self.fake_sampler: bool = fake_sampler

        # Update the global seed for numpy/random/tf-eager/torch if we are not
        # the local worker, otherwise, this was already done in the Trainer
        # object itself.
        if self.worker_index > 0:
            update_global_seed_if_necessary(
                policy_config.get("framework"), seed)

        # A single environment provided by the user (via config.env). This may
        # also remain None.
        # 1) Create the env using the user provided env_creator. This may
        #    return a gym.Env (incl. MultiAgentEnv), an already vectorized
        #    VectorEnv, BaseEnv, ExternalEnv, or an ActorHandle (remote env).
        # 2) Wrap - if applicable - with Atari/recording/rendering wrappers.
        # 3) Seed the env, if necessary.
        # 4) Vectorize the existing single env by creating more clones of
        #    this env and wrapping it with the RLlib BaseEnv class.
        self.env = None

        # Create a (single) env for this worker.
        if not (worker_index == 0 and num_workers > 0
                and not policy_config.get("create_env_on_driver")):
            # Run the `env_creator` function passing the EnvContext.
            self.env = env_creator(copy.deepcopy(self.env_context))

        if self.env is not None:
            # Validate environment (general validation function).
            _validate_env(self.env, env_context=self.env_context)
            # Custom validation function given.
            if validate_env is not None:
                validate_env(self.env, self.env_context)
            # We can't auto-wrap a BaseEnv.
            if isinstance(self.env, (BaseEnv, ray.actor.ActorHandle)):

                def wrap(env):
                    return env

            # Atari type env and "deepmind" preprocessor pref.
            elif is_atari(self.env) and \
                    not model_config.get("custom_preprocessor") and \
                    preprocessor_pref == "deepmind":

                # Deepmind wrappers already handle all preprocessing.
                self.preprocessing_enabled = False

                # If clip_rewards not explicitly set to False, switch it
                # on here (clip between -1.0 and 1.0).
                if clip_rewards is None:
                    clip_rewards = True

                # Framestacking is used.
                use_framestack = model_config.get("framestack") is True

                def wrap(env):
                    env = wrap_deepmind(
                        env,
                        dim=model_config.get("dim"),
                        framestack=use_framestack)
                    env = record_env_wrapper(env, record_env, log_dir,
                                             policy_config)
                    return env

            # gym.Env -> Wrap with gym Monitor.
            else:

                def wrap(env):
                    return record_env_wrapper(env, record_env, log_dir,
                                              policy_config)

            # Wrap env through the correct wrapper.
            self.env: EnvType = wrap(self.env)
            # Ideally, we would use the same make_sub_env() function below
            # to create self.env, but wrap(env) and self.env has a cyclic
            # dependency on each other right now, so we would settle on
            # duplicating the random seed setting logic for now.
            _update_env_seed_if_necessary(self.env, seed, worker_index, 0)

        def make_sub_env(vector_index):
            # Used to created additional environments during environment
            # vectorization.

            # Create the env context (config dict + meta-data) for
            # this particular sub-env within the vectorized one.
            env_ctx = env_context.copy_with_overrides(
                worker_index=worker_index,
                vector_index=vector_index,
                remote=remote_worker_envs)
            # Create the sub-env.
            env = env_creator(env_ctx)
            # Validate first.
            _validate_env(env, env_context=env_ctx)
            # Custom validation function given by user.
            if validate_env is not None:
                validate_env(env, env_ctx)
            # Use our wrapper, defined above.
            env = wrap(env)

            # Make sure a deterministic random seed is set on
            # all the sub-environments if specified.
            _update_env_seed_if_necessary(env, seed, worker_index,
                                          vector_index)
            return env

        self.make_sub_env_fn = make_sub_env
        self.spaces = spaces

        policy_dict = _determine_spaces_for_multi_agent_dict(
            policy_spec,
            self.env,
            spaces=self.spaces,
            policy_config=policy_config)

        # List of IDs of those policies, which should be trained.
        # By default, these are all policies found in the policy_dict.
        self.policies_to_train: List[PolicyID] = policies_to_train or list(
            policy_dict.keys())
        self.set_policies_to_train(self.policies_to_train)

        self.policy_map: PolicyMap = None
        self.preprocessors: Dict[PolicyID, Preprocessor] = None

        # Check available number of GPUs.
        num_gpus = policy_config.get("num_gpus", 0) if \
            self.worker_index == 0 else \
            policy_config.get("num_gpus_per_worker", 0)
        # Error if we don't find enough GPUs.
        if ray.is_initialized() and \
                ray.worker._mode() != ray.worker.LOCAL_MODE and \
                not policy_config.get("_fake_gpus"):

            devices = []
            if policy_config.get("framework") in ["tf2", "tf", "tfe"]:
                devices = get_tf_gpu_devices()
            elif policy_config.get("framework") == "torch":
                devices = list(range(torch.cuda.device_count()))

            if len(devices) < num_gpus:
                raise RuntimeError(
                    ERR_MSG_NO_GPUS.format(len(devices), devices) +
                    HOWTO_CHANGE_CONFIG)
        # Warn, if running in local-mode and actual GPUs (not faked) are
        # requested.
        elif ray.is_initialized() and \
                ray.worker._mode() == ray.worker.LOCAL_MODE and \
                num_gpus > 0 and not policy_config.get("_fake_gpus"):
            logger.warning(
                "You are running ray with `local_mode=True`, but have "
                f"configured {num_gpus} GPUs to be used! In local mode, "
                f"Policies are placed on the CPU and the `num_gpus` setting "
                f"is ignored.")

        self._build_policy_map(
            policy_dict,
            policy_config,
            session_creator=tf_session_creator,
            seed=seed)

        # Update Policy's view requirements from Model, only if Policy directly
        # inherited from base `Policy` class. At this point here, the Policy
        # must have it's Model (if any) defined and ready to output an initial
        # state.
        for pol in self.policy_map.values():
            if not pol._model_init_state_automatically_added:
                pol._update_model_view_requirements_from_init_state()

        self.multiagent: bool = set(
            self.policy_map.keys()) != {DEFAULT_POLICY_ID}
        if self.multiagent and self.env is not None:
            if not isinstance(self.env,
                              (BaseEnv, ExternalMultiAgentEnv, MultiAgentEnv,
                               ray.actor.ActorHandle)):
                raise ValueError(
                    f"Have multiple policies {self.policy_map}, but the "
                    f"env {self.env} is not a subclass of BaseEnv, "
                    f"MultiAgentEnv, ActorHandle, or ExternalMultiAgentEnv!")

        self.filters: Dict[PolicyID, Filter] = {
            policy_id: get_filter(self.observation_filter,
                                  policy.observation_space.shape)
            for (policy_id, policy) in self.policy_map.items()
        }
        if self.worker_index == 0:
            logger.info("Built filter map: {}".format(self.filters))

        # Vectorize environment, if any.
        self.num_envs: int = num_envs
        # This RolloutWorker has no env.
        if self.env is None:
            self.async_env = None
        # Use a custom env-vectorizer and call it providing self.env.
        elif "custom_vector_env" in policy_config:
            self.async_env = policy_config["custom_vector_env"](self.env)
        # Default: Vectorize self.env via the make_sub_env function. This adds
        # further clones of self.env and creates a RLlib BaseEnv (which is
        # vectorized under the hood).
        else:
            # Always use vector env for consistency even if num_envs = 1.
            self.async_env: BaseEnv = BaseEnv.to_base_env(
                self.env,
                make_env=self.make_sub_env_fn,
                num_envs=num_envs,
                remote_envs=remote_worker_envs,
                remote_env_batch_wait_ms=remote_env_batch_wait_ms,
                policy_config=policy_config,
            )

        # `truncate_episodes`: Allow a batch to contain more than one episode
        # (fragments) and always make the batch `rollout_fragment_length`
        # long.
        if self.batch_mode == "truncate_episodes":
            pack = True
        # `complete_episodes`: Never cut episodes and sampler will return
        # exactly one (complete) episode per poll.
        elif self.batch_mode == "complete_episodes":
            rollout_fragment_length = float("inf")
            pack = False
        else:
            raise ValueError("Unsupported batch mode: {}".format(
                self.batch_mode))

        # Create the IOContext for this worker.
        self.io_context: IOContext = IOContext(log_dir, policy_config,
                                               worker_index, self)
        self.reward_estimators: List[OffPolicyEstimator] = []
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

        render = False
        if policy_config.get("render_env") is True and \
                (num_workers == 0 or worker_index == 1):
            render = True

        if self.env is None:
            self.sampler = None
        elif sample_async:
            self.sampler = AsyncSampler(
                worker=self,
                env=self.async_env,
                clip_rewards=clip_rewards,
                rollout_fragment_length=rollout_fragment_length,
                count_steps_by=count_steps_by,
                callbacks=self.callbacks,
                horizon=episode_horizon,
                multiple_episodes_in_batch=pack,
                normalize_actions=normalize_actions,
                clip_actions=clip_actions,
                blackhole_outputs="simulation" in input_evaluation,
                soft_horizon=soft_horizon,
                no_done_at_end=no_done_at_end,
                observation_fn=observation_fn,
                sample_collector_class=policy_config.get("sample_collector"),
                render=render,
            )
            # Start the Sampler thread.
            self.sampler.start()
        else:
            self.sampler = SyncSampler(
                worker=self,
                env=self.async_env,
                clip_rewards=clip_rewards,
                rollout_fragment_length=rollout_fragment_length,
                count_steps_by=count_steps_by,
                callbacks=self.callbacks,
                horizon=episode_horizon,
                multiple_episodes_in_batch=pack,
                normalize_actions=normalize_actions,
                clip_actions=clip_actions,
                soft_horizon=soft_horizon,
                no_done_at_end=no_done_at_end,
                observation_fn=observation_fn,
                sample_collector_class=policy_config.get("sample_collector"),
                render=render,
            )

        self.input_reader: InputReader = input_creator(self.io_context)
        self.output_writer: OutputWriter = output_creator(self.io_context)

        logger.debug(
            "Created rollout worker with env {} ({}), policies {}".format(
                self.async_env, self.env, self.policy_map))

    @DeveloperAPI
    def sample(self) -> SampleBatchType:
        """Returns a batch of experience sampled from this worker.

        This method must be implemented by subclasses.

        Returns:
            SampleBatchType: A columnar batch of experiences (e.g., tensors).

        Examples:
            >>> print(worker.sample())
            SampleBatch({"obs": [1, 2, 3], "action": [0, 1, 0], ...})
        """

        if self.fake_sampler and self.last_batch is not None:
            return self.last_batch
        elif self.input_reader is None:
            raise ValueError("RolloutWorker has no `input_reader` object! "
                             "Cannot call `sample()`. You can try setting "
                             "`create_env_on_driver` to True.")

        if log_once("sample_start"):
            logger.info("Generating sample batch of size {}".format(
                self.rollout_fragment_length))

        batches = [self.input_reader.next()]
        steps_so_far = batches[0].count if \
            self.count_steps_by == "env_steps" else \
            batches[0].agent_steps()

        # In truncate_episodes mode, never pull more than 1 batch per env.
        # This avoids over-running the target batch size.
        if self.batch_mode == "truncate_episodes":
            max_batches = self.num_envs
        else:
            max_batches = float("inf")

        while (steps_so_far < self.rollout_fragment_length
               and len(batches) < max_batches):
            batch = self.input_reader.next()
            steps_so_far += batch.count if \
                self.count_steps_by == "env_steps" else \
                batch.agent_steps()
            batches.append(batch)
        batch = batches[0].concat_samples(batches) if len(batches) > 1 else \
            batches[0]

        self.callbacks.on_sample_end(worker=self, samples=batch)

        # Always do writes prior to compression for consistency and to allow
        # for better compression inside the writer.
        self.output_writer.write(batch)

        # Do off-policy estimation, if needed.
        if self.reward_estimators:
            for sub_batch in batch.split_by_episode():
                for estimator in self.reward_estimators:
                    estimator.process(sub_batch)

        if log_once("sample_end"):
            logger.info("Completed sample batch:\n\n{}\n".format(
                summarize(batch)))

        if self.compress_observations:
            batch.compress(bulk=self.compress_observations == "bulk")

        if self.fake_sampler:
            self.last_batch = batch
        return batch

    @DeveloperAPI
    @ray.method(num_returns=2)
    def sample_with_count(self) -> Tuple[SampleBatchType, int]:
        """Same as sample() but returns the count as a separate future."""
        batch = self.sample()
        return batch, batch.count

    @DeveloperAPI
    def get_weights(
            self,
            policies: Optional[List[PolicyID]] = None,
    ) -> Dict[PolicyID, ModelWeights]:
        """Returns the model weights of this worker.

        Args:
            policies (Optional[List[PolicyID]]): List of PolicyIDs to get
                the weights from. Use None for all policies.

        Returns:
            Dict[PolicyID, ModelWeights]: Mapping from PolicyIDs to weights
                dicts.
        """
        if policies is None:
            policies = list(self.policy_map.keys())
        policies = force_list(policies)

        return {
            pid: policy.get_weights()
            for pid, policy in self.policy_map.items() if pid in policies
        }

    @DeveloperAPI
    def set_weights(self, weights: ModelWeights,
                    global_vars: dict = None) -> None:
        """Sets the model weights of this worker.

        Examples:
            >>> weights = worker.get_weights()
            >>> worker.set_weights(weights)
        """
        for pid, w in weights.items():
            self.policy_map[pid].set_weights(w)
        if global_vars:
            self.set_global_vars(global_vars)

    @DeveloperAPI
    def compute_gradients(
            self, samples: SampleBatchType) -> Tuple[ModelGradients, dict]:
        """Returns a gradient computed w.r.t the specified samples.

        Returns:
            (grads, info): A list of gradients that can be applied on a
            compatible worker. In the multi-agent case, returns a dict
            of gradients keyed by policy ids. An info dictionary of
            extra metadata is also returned.

        Examples:
            >>> batch = worker.sample()
            >>> grads, info = worker.compute_gradients(samples)
        """
        if log_once("compute_gradients"):
            logger.info("Compute gradients on:\n\n{}\n".format(
                summarize(samples)))
        if isinstance(samples, MultiAgentBatch):
            grad_out, info_out = {}, {}
            if self.tf_sess is not None:
                builder = TFRunBuilder(self.tf_sess, "compute_gradients")
                for pid, batch in samples.policy_batches.items():
                    if pid not in self.policies_to_train:
                        continue
                    grad_out[pid], info_out[pid] = (
                        self.policy_map[pid]._build_compute_gradients(
                            builder, batch))
                grad_out = {k: builder.get(v) for k, v in grad_out.items()}
                info_out = {k: builder.get(v) for k, v in info_out.items()}
            else:
                for pid, batch in samples.policy_batches.items():
                    if pid not in self.policies_to_train:
                        continue
                    grad_out[pid], info_out[pid] = (
                        self.policy_map[pid].compute_gradients(batch))
        else:
            grad_out, info_out = (
                self.policy_map[DEFAULT_POLICY_ID].compute_gradients(samples))
        info_out["batch_count"] = samples.count
        if log_once("grad_out"):
            logger.info("Compute grad info:\n\n{}\n".format(
                summarize(info_out)))
        return grad_out, info_out

    @DeveloperAPI
    def apply_gradients(self, grads: ModelGradients) -> Dict[PolicyID, Any]:
        """Applies the given gradients to this worker's weights.

        Examples:
            >>> samples = worker.sample()
            >>> grads, info = worker.compute_gradients(samples)
            >>> worker.apply_gradients(grads)
        """
        if log_once("apply_gradients"):
            logger.info("Apply gradients:\n\n{}\n".format(summarize(grads)))
        if isinstance(grads, dict):
            if self.tf_sess is not None:
                builder = TFRunBuilder(self.tf_sess, "apply_gradients")
                outputs = {
                    pid: self.policy_map[pid]._build_apply_gradients(
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

    @DeveloperAPI
    def learn_on_batch(self, samples: SampleBatchType) -> dict:
        """Update policies based on the given batch.

        This is the equivalent to apply_gradients(compute_gradients(samples)),
        but can be optimized to avoid pulling gradients into CPU memory.

        Returns:
            info: dictionary of extra metadata from compute_gradients().

        Examples:
            >>> batch = worker.sample()
            >>> worker.learn_on_batch(samples)
        """
        if log_once("learn_on_batch"):
            logger.info(
                "Training on concatenated sample batches:\n\n{}\n".format(
                    summarize(samples)))
        if isinstance(samples, MultiAgentBatch):
            info_out = {}
            builders = {}
            to_fetch = {}
            for pid, batch in samples.policy_batches.items():
                if pid not in self.policies_to_train:
                    continue
                # Decompress SampleBatch, in case some columns are compressed.
                batch.decompress_if_needed()
                policy = self.policy_map[pid]
                tf_session = policy.get_session()
                if tf_session and hasattr(policy, "_build_learn_on_batch"):
                    builders[pid] = TFRunBuilder(tf_session, "learn_on_batch")
                    to_fetch[pid] = policy._build_learn_on_batch(
                        builders[pid], batch)
                else:
                    info_out[pid] = policy.learn_on_batch(batch)
            info_out.update(
                {pid: builders[pid].get(v)
                 for pid, v in to_fetch.items()})
        else:
            info_out = {
                DEFAULT_POLICY_ID: self.policy_map[DEFAULT_POLICY_ID]
                .learn_on_batch(samples)
            }
        if log_once("learn_out"):
            logger.debug("Training out:\n\n{}\n".format(summarize(info_out)))
        return info_out

    def sample_and_learn(self, expected_batch_size: int, num_sgd_iter: int,
                         sgd_minibatch_size: str,
                         standardize_fields: List[str]) -> Tuple[dict, int]:
        """Sample and batch and learn on it.

        This is typically used in combination with distributed allreduce.

        Args:
            expected_batch_size (int): Expected number of samples to learn on.
            num_sgd_iter (int): Number of SGD iterations.
            sgd_minibatch_size (int): SGD minibatch size.
            standardize_fields (list): List of sample fields to normalize.

        Returns:
            info: dictionary of extra metadata from learn_on_batch().
            count: number of samples learned on.
        """
        batch = self.sample()
        assert batch.count == expected_batch_size, \
            ("Batch size possibly out of sync between workers, expected:",
             expected_batch_size, "got:", batch.count)
        logger.info("Executing distributed minibatch SGD "
                    "with epoch size {}, minibatch size {}".format(
                        batch.count, sgd_minibatch_size))
        info = do_minibatch_sgd(batch, self.policy_map, self, num_sgd_iter,
                                sgd_minibatch_size, standardize_fields)
        return info, batch.count

    @DeveloperAPI
    def get_metrics(self) -> List[Union[RolloutMetrics, OffPolicyEstimate]]:
        """Returns a list of new RolloutMetric objects from evaluation."""

        # Get metrics from sampler (if any).
        if self.sampler is not None:
            out = self.sampler.get_metrics()
        else:
            out = []
        # Get metrics from our reward-estimators (if any).
        for m in self.reward_estimators:
            out.extend(m.get_metrics())

        return out

    @DeveloperAPI
    def foreach_env(self, func: Callable[[BaseEnv], T]) -> List[T]:
        """Apply the given function to each underlying env instance."""

        if self.async_env is None:
            return []

        envs = self.async_env.get_unwrapped()
        # Empty list (not implemented): Call function directly on the
        # BaseEnv.
        if not envs:
            return [func(self.async_env)]
        # Call function on all underlying (vectorized) envs.
        else:
            return [func(e) for e in envs]

    @DeveloperAPI
    def foreach_env_with_context(
            self, func: Callable[[BaseEnv, EnvContext], T]) -> List[T]:
        """Apply the given function to each underlying env instance."""

        if self.async_env is None:
            return []

        envs = self.async_env.get_unwrapped()
        # Empty list (not implemented): Call function directly on the
        # BaseEnv.
        if not envs:
            return [func(self.async_env, self.env_context)]
        # Call function on all underlying (vectorized) envs.
        else:
            ret = []
            for i, e in enumerate(envs):
                ctx = self.env_context.copy_with_overrides(vector_index=i)
                ret.append(func(e, ctx))
            return ret

    @DeveloperAPI
    def get_policy(self, policy_id: PolicyID = DEFAULT_POLICY_ID) -> Policy:
        """Return policy for the specified id, or None.

        Args:
            policy_id (PolicyID): ID of the policy to return.

        Returns:
            Optional[Policy]: The policy under the given ID (or None if not
                found).
        """

        return self.policy_map.get(policy_id)

    @DeveloperAPI
    def add_policy(
            self,
            *,
            policy_id: PolicyID,
            policy_cls: Type[Policy],
            observation_space: Optional[gym.spaces.Space] = None,
            action_space: Optional[gym.spaces.Space] = None,
            config: Optional[PartialTrainerConfigDict] = None,
            policy_mapping_fn: Optional[Callable[
                [AgentID, "MultiAgentEpisode"], PolicyID]] = None,
            policies_to_train: Optional[List[PolicyID]] = None,
    ) -> Policy:
        """Adds a new policy to this RolloutWorker.

        Args:
            policy_id (Optional[PolicyID]): ID of the policy to add.
            policy_cls (Type[Policy]): The Policy class to use for
                constructing the new Policy.
            observation_space (Optional[gym.spaces.Space]): The observation
                space of the policy to add.
            action_space (Optional[gym.spaces.Space]): The action space
                of the policy to add.
            config (Optional[PartialTrainerConfigDict]): The config
                overrides for the policy to add.
            policy_config (Optional[TrainerConfigDict]): The base config of the
                Trainer object owning this RolloutWorker.
            policy_mapping_fn (Optional[Callable[[AgentID, MultiAgentEpisode],
                PolicyID]]): An optional (updated) policy mapping function to
                use from here on. Note that already ongoing episodes will not
                change their mapping but will use the old mapping till the
                end of the episode.
            policies_to_train (Optional[List[PolicyID]]): An optional list of
                policy IDs to be trained. If None, will keep the existing list
                in place. Policies, whose IDs are not in the list will not be
                updated.

        Returns:
            Policy: The newly added policy (the copy that got added to the
                local worker).
        """
        if policy_id in self.policy_map:
            raise ValueError(f"Policy ID '{policy_id}' already in policy map!")
        policy_dict = _determine_spaces_for_multi_agent_dict(
            {
                policy_id: PolicySpec(policy_cls, observation_space,
                                      action_space, config or {})
            },
            self.env,
            spaces=self.spaces,
            policy_config=self.policy_config,
        )
        self._build_policy_map(
            policy_dict,
            self.policy_config,
            seed=self.policy_config.get("seed"))
        new_policy = self.policy_map[policy_id]

        self.filters[policy_id] = get_filter(
            self.observation_filter, new_policy.observation_space.shape)

        self.set_policy_mapping_fn(policy_mapping_fn)
        self.set_policies_to_train(policies_to_train)

        return new_policy

    @DeveloperAPI
    def remove_policy(
            self,
            *,
            policy_id: PolicyID = DEFAULT_POLICY_ID,
            policy_mapping_fn: Optional[Callable[[AgentID], PolicyID]] = None,
            policies_to_train: Optional[List[PolicyID]] = None,
    ):
        """Removes a policy from this RolloutWorker.

        Args:
            policy_id (Optional[PolicyID]): ID of the policy to be removed.
            policy_mapping_fn (Optional[Callable[[AgentID], PolicyID]]): An
                optional (updated) policy mapping function to use from here on.
                Note that already ongoing episodes will not change their
                mapping but will use the old mapping till the end of the
                episode.
            policies_to_train (Optional[List[PolicyID]]): An optional list of
                policy IDs to be trained. If None, will keep the existing list
                in place. Policies, whose IDs are not in the list will not be
                updated.
        """
        if policy_id not in self.policy_map:
            raise ValueError(f"Policy ID '{policy_id}' not in policy map!")
        del self.policy_map[policy_id]
        del self.preprocessors[policy_id]
        self.set_policy_mapping_fn(policy_mapping_fn)
        self.set_policies_to_train(policies_to_train)

    @DeveloperAPI
    def set_policy_mapping_fn(
            self,
            policy_mapping_fn: Optional[Callable[
                [AgentID, "MultiAgentEpisode"], PolicyID]] = None,
    ):
        """Sets `self.policy_mapping_fn` to a new callable (if provided).

        Args:
            policy_mapping_fn (Optional[Callable[[AgentID], PolicyID]]): The
                new mapping function to use. If None, will keep the existing
                mapping function in place.
        """
        if policy_mapping_fn is not None:
            self.policy_mapping_fn = policy_mapping_fn
            if not callable(self.policy_mapping_fn):
                raise ValueError("`policy_mapping_fn` must be a callable!")

    @DeveloperAPI
    def set_policies_to_train(
            self, policies_to_train: Optional[List[PolicyID]] = None):
        """Sets `self.policies_to_train` to a new list of PolicyIDs.

        Args:
            policies_to_train (Optional[List[PolicyID]]): The new
                list of policy IDs to train with. If None, will keep the
                existing list in place.
        """
        if policies_to_train is not None:
            self.policies_to_train = policies_to_train

    @DeveloperAPI
    def for_policy(self,
                   func: Callable[[Policy], T],
                   policy_id: Optional[PolicyID] = DEFAULT_POLICY_ID,
                   **kwargs) -> T:
        """Apply the given function to the specified policy."""

        return func(self.policy_map[policy_id], **kwargs)

    @DeveloperAPI
    def foreach_policy(self, func: Callable[[Policy, PolicyID], T],
                       **kwargs) -> List[T]:
        """Apply the given function to each (policy, policy_id) tuple."""

        return [
            func(policy, pid, **kwargs)
            for pid, policy in self.policy_map.items()
        ]

    @DeveloperAPI
    def foreach_trainable_policy(self, func: Callable[[Policy, PolicyID], T],
                                 **kwargs) -> List[T]:
        """
        Applies the given function to each (policy, policy_id) tuple, which
        can be found in `self.policies_to_train`.

        Args:
            func (callable): A function - taking a Policy and its ID - that is
                called on all Policies within `self.policies_to_train`.

        Returns:
            List[any]: The list of n return values of all
                `func([policy], [ID])`-calls.
        """
        return [
            func(policy, pid, **kwargs)
            for pid, policy in self.policy_map.items()
            if pid in self.policies_to_train
        ]

    @DeveloperAPI
    def sync_filters(self, new_filters: dict) -> None:
        """Changes self's filter to given and rebases any accumulated delta.

        Args:
            new_filters (dict): Filters with new state to update local copy.
        """
        assert all(k in new_filters for k in self.filters)
        for k in self.filters:
            self.filters[k].sync(new_filters[k])

    @DeveloperAPI
    def get_filters(self, flush_after: bool = False) -> dict:
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

    @DeveloperAPI
    def save(self) -> bytes:
        filters = self.get_filters(flush_after=True)
        state = {}
        policy_specs = {}
        for pid in self.policy_map:
            state[pid] = self.policy_map[pid].get_state()
            policy_specs[pid] = self.policy_map.policy_specs[pid]
        return pickle.dumps({
            "filters": filters,
            "state": state,
            "policy_specs": policy_specs,
        })

    @DeveloperAPI
    def restore(self, objs: bytes) -> None:
        objs = pickle.loads(objs)
        self.sync_filters(objs["filters"])
        for pid, state in objs["state"].items():
            if pid not in self.policy_map:
                pol_spec = objs.get("policy_specs", {}).get(pid)
                if not pol_spec:
                    logger.warning(
                        f"PolicyID '{pid}' was probably added on-the-fly (not"
                        " part of the static `multagent.policies` config) and"
                        " no PolicySpec objects found in the pickled policy "
                        "state. Will not add `{pid}`, but ignore it for now.")
                else:
                    self.add_policy(
                        policy_id=pid,
                        policy_cls=pol_spec.policy_class,
                        observation_space=pol_spec.observation_space,
                        action_space=pol_spec.action_space,
                        config=pol_spec.config,
                    )
            else:
                self.policy_map[pid].set_state(state)

    @DeveloperAPI
    def set_global_vars(self, global_vars: dict) -> None:
        self.foreach_policy(lambda p, _: p.on_global_var_update(global_vars))
        self.global_vars = global_vars

    @DeveloperAPI
    def get_global_vars(self) -> dict:
        return self.global_vars

    @DeveloperAPI
    def export_policy_model(self,
                            export_dir: str,
                            policy_id: PolicyID = DEFAULT_POLICY_ID,
                            onnx: Optional[int] = None):
        self.policy_map[policy_id].export_model(export_dir, onnx=onnx)

    @DeveloperAPI
    def import_policy_model_from_h5(self,
                                    import_file: str,
                                    policy_id: PolicyID = DEFAULT_POLICY_ID):
        self.policy_map[policy_id].import_model_from_h5(import_file)

    @DeveloperAPI
    def export_policy_checkpoint(self,
                                 export_dir: str,
                                 filename_prefix: str = "model",
                                 policy_id: PolicyID = DEFAULT_POLICY_ID):
        self.policy_map[policy_id].export_checkpoint(export_dir,
                                                     filename_prefix)

    @DeveloperAPI
    def stop(self) -> None:
        if self.env is not None:
            self.async_env.stop()

    @DeveloperAPI
    def creation_args(self) -> dict:
        """Returns the args used to create this worker."""
        return self._original_kwargs

    @DeveloperAPI
    def get_host(self) -> str:
        """Returns the hostname of the process running this evaluator."""

        return platform.node()

    @DeveloperAPI
    def apply(self, func: Callable[["RolloutWorker"], T], *args) -> T:
        """Apply the given function to this rollout worker instance."""

        return func(self, *args)

    def _build_policy_map(
            self,
            policy_dict: MultiAgentPolicyConfigDict,
            policy_config: TrainerConfigDict,
            session_creator: Optional[Callable[[], "tf1.Session"]] = None,
            seed: Optional[int] = None,
    ) -> Tuple[Dict[PolicyID, Policy], Dict[PolicyID, Preprocessor]]:

        ma_config = policy_config.get("multiagent", {})

        self.policy_map = self.policy_map or PolicyMap(
            worker_index=self.worker_index,
            num_workers=self.num_workers,
            capacity=ma_config.get("policy_map_capacity"),
            path=ma_config.get("policy_map_cache"),
            policy_config=policy_config,
            session_creator=session_creator,
            seed=seed,
        )
        self.preprocessors = self.preprocessors or {}

        for name, (orig_cls, obs_space, act_space,
                   conf) in sorted(policy_dict.items()):
            logger.debug("Creating policy for {}".format(name))
            merged_conf = merge_dicts(policy_config, conf or {})
            merged_conf["num_workers"] = self.num_workers
            merged_conf["worker_index"] = self.worker_index
            if self.preprocessing_enabled:
                preprocessor = ModelCatalog.get_preprocessor_for_space(
                    obs_space, merged_conf.get("model"))
                self.preprocessors[name] = preprocessor
                if preprocessor is not None:
                    obs_space = preprocessor.observation_space
            else:
                self.preprocessors[name] = NoPreprocessor(obs_space)

            if isinstance(obs_space, (gym.spaces.Dict, gym.spaces.Tuple)):
                raise ValueError(
                    "Found raw Tuple|Dict space as input to policy. "
                    "Please preprocess these observations with a "
                    "Tuple|DictFlatteningPreprocessor.")

            self.policy_map.create_policy(name, orig_cls, obs_space, act_space,
                                          conf, merged_conf)

        if self.worker_index == 0:
            logger.info(f"Built policy map: {self.policy_map}")
            logger.info(f"Built preprocessor map: {self.preprocessors}")

    def setup_torch_data_parallel(self, url: str, world_rank: int,
                                  world_size: int, backend: str) -> None:
        """Join a torch process group for distributed SGD."""

        logger.info("Joining process group, url={}, world_rank={}, "
                    "world_size={}, backend={}".format(url, world_rank,
                                                       world_size, backend))
        torch.distributed.init_process_group(
            backend=backend,
            init_method=url,
            rank=world_rank,
            world_size=world_size)

        for pid, policy in self.policy_map.items():
            if not isinstance(policy, TorchPolicy):
                raise ValueError(
                    "This policy does not support torch distributed", policy)
            policy.distributed_world_size = world_size

    def get_node_ip(self) -> str:
        """Returns the IP address of the current node."""
        return ray.util.get_node_ip_address()

    def find_free_port(self) -> int:
        """Finds a free port on the current node."""
        from ray.util.sgd import utils
        return utils.find_free_port()

    def __del__(self):
        if hasattr(self, "sampler") and isinstance(self.sampler, AsyncSampler):
            self.sampler.shutdown = True


def _determine_spaces_for_multi_agent_dict(
        multi_agent_dict: MultiAgentPolicyConfigDict,
        env: Optional[EnvType] = None,
        spaces: Optional[Dict[PolicyID, Tuple[gym.spaces.Space,
                                              gym.spaces.Space]]] = None,
        policy_config: Optional[PartialTrainerConfigDict] = None,
) -> MultiAgentPolicyConfigDict:

    policy_config = policy_config or {}

    # Try extracting spaces from env or from given spaces dict.
    env_obs_space = None
    env_act_space = None

    # Env is a ray.remote: Get spaces via its (automatically added)
    # `_get_spaces()` method.
    if isinstance(env, ray.actor.ActorHandle):
        env_obs_space, env_act_space = ray.get(env._get_spaces.remote())
    # Normal env (gym.Env or MultiAgentEnv): These should have the
    # `observation_space` and `action_space` properties.
    elif env is not None:
        if hasattr(env, "observation_space") and isinstance(
                env.observation_space, gym.Space):
            env_obs_space = env.observation_space

        if hasattr(env, "action_space") and isinstance(env.action_space,
                                                       gym.Space):
            env_act_space = env.action_space
    # Last resort: Try getting the env's spaces from the spaces
    # dict's special __env__ key.
    if spaces is not None:
        if env_obs_space is None:
            env_obs_space = spaces.get("__env__", [None])[0]
        if env_act_space is None:
            env_act_space = spaces.get("__env__", [None, None])[1]

    for pid, policy_spec in multi_agent_dict.copy().items():
        if policy_spec.observation_space is None:
            if spaces is not None and pid in spaces:
                obs_space = spaces[pid][0]
            elif env_obs_space is not None:
                obs_space = env_obs_space
            elif policy_config.get("observation_space"):
                obs_space = policy_config["observation_space"]
            else:
                raise ValueError(
                    "`observation_space` not provided in PolicySpec for "
                    f"{pid} and env does not have an observation space OR "
                    "no spaces received from other workers' env(s) OR no "
                    "`observation_space` specified in config!")

            multi_agent_dict[pid] = multi_agent_dict[pid]._replace(
                observation_space=obs_space)

        if policy_spec.action_space is None:
            if spaces is not None and pid in spaces:
                act_space = spaces[pid][1]
            elif env_act_space is not None:
                act_space = env_act_space
            elif policy_config.get("action_space"):
                act_space = policy_config["action_space"]
            else:
                raise ValueError(
                    "`action_space` not provided in PolicySpec for "
                    f"{pid} and env does not have an action space OR "
                    "no spaces received from other workers' env(s) OR no "
                    "`action_space` specified in config!")
            multi_agent_dict[pid] = multi_agent_dict[pid]._replace(
                action_space=act_space)
    return multi_agent_dict


def _validate_env(env: EnvType, env_context: EnvContext = None):
    # Base message for checking the env for vector-index=0
    msg = f"Validating sub-env at vector index={env_context.vector_index} ..."

    allowed_types = [
        gym.Env, ExternalEnv, VectorEnv, BaseEnv, ray.actor.ActorHandle
    ]
    if not any(isinstance(env, tpe) for tpe in allowed_types):
        # Allow this as a special case (assumed gym.Env).
        # TODO: Disallow this early-out. Everything should conform to a few
        #  supported classes, i.e. gym.Env/MultiAgentEnv/etc...
        if hasattr(env, "observation_space") and hasattr(env, "action_space"):
            logger.warning(msg + f" (warning; invalid env-type={type(env)})")
            return
        else:
            logger.warning(msg + " (NOT OK)")
            raise EnvError(
                "Returned env should be an instance of gym.Env (incl. "
                "MultiAgentEnv), ExternalEnv, VectorEnv, or BaseEnv. "
                f"The provided env creator function returned {env} "
                f"(type={type(env)}).")

    # Do some test runs with the provided env.
    if isinstance(env, gym.Env) and not isinstance(env, MultiAgentEnv):
        # Make sure the gym.Env has the two space attributes properly set.
        assert hasattr(env, "observation_space") and hasattr(
            env, "action_space")
        # Get a dummy observation by resetting the env.
        dummy_obs = env.reset()
        # Convert lists to np.ndarrays.
        if type(dummy_obs) is list and isinstance(env.observation_space,
                                                  gym.spaces.Box):
            dummy_obs = np.array(dummy_obs)
        # Ignore float32/float64 diffs.
        if isinstance(env.observation_space, gym.spaces.Box) and \
                env.observation_space.dtype != dummy_obs.dtype:
            dummy_obs = dummy_obs.astype(env.observation_space.dtype)
        # Check, if observation is ok (part of the observation space). If not,
        # error.
        if not env.observation_space.contains(dummy_obs):
            logger.warning(msg + " (NOT OK)")
            raise EnvError(
                f"Env's `observation_space` {env.observation_space} does not "
                f"contain returned observation after a reset ({dummy_obs})!")

    # Log that everything is ok.
    logger.info(msg + " (ok)")

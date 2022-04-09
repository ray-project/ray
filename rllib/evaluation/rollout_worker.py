import copy
import gym
from gym.spaces import Discrete, MultiDiscrete, Space
import logging
import numpy as np
import platform
import os
import tree  # pip install dm_tree
from typing import (
    Any,
    Callable,
    Container,
    Dict,
    List,
    Optional,
    Set,
    Tuple,
    Type,
    TYPE_CHECKING,
    Union,
)

import ray
from ray import ObjectRef
from ray import cloudpickle as pickle
from ray.rllib.env.base_env import BaseEnv, convert_to_base_env
from ray.rllib.env.env_context import EnvContext
from ray.rllib.env.multi_agent_env import MultiAgentEnv
from ray.rllib.env.external_multi_agent_env import ExternalMultiAgentEnv
from ray.rllib.env.utils import record_env_wrapper
from ray.rllib.env.wrappers.atari_wrappers import wrap_deepmind, is_atari
from ray.rllib.evaluation.sampler import AsyncSampler, SyncSampler
from ray.rllib.evaluation.metrics import RolloutMetrics
from ray.rllib.models import ModelCatalog
from ray.rllib.models.preprocessors import Preprocessor
from ray.rllib.offline import NoopOutput, IOContext, OutputWriter, InputReader
from ray.rllib.offline.off_policy_estimator import OffPolicyEstimator, OffPolicyEstimate
from ray.rllib.offline.is_estimator import ImportanceSamplingEstimator
from ray.rllib.offline.wis_estimator import WeightedImportanceSamplingEstimator
from ray.rllib.policy.sample_batch import MultiAgentBatch, DEFAULT_POLICY_ID
from ray.rllib.policy.policy import Policy, PolicySpec
from ray.rllib.policy.policy_map import PolicyMap
from ray.rllib.policy.torch_policy import TorchPolicy
from ray.rllib.utils import force_list, merge_dicts, check_env
from ray.rllib.utils.annotations import Deprecated, DeveloperAPI, ExperimentalAPI
from ray.rllib.utils.debug import summarize, update_global_seed_if_necessary
from ray.rllib.utils.deprecation import deprecation_warning
from ray.rllib.utils.error import ERR_MSG_NO_GPUS, HOWTO_CHANGE_CONFIG
from ray.rllib.utils.filter import get_filter, Filter
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.sgd import do_minibatch_sgd
from ray.rllib.utils.tf_utils import get_gpu_devices as get_tf_gpu_devices
from ray.rllib.utils.tf_run_builder import TFRunBuilder
from ray.rllib.utils.typing import (
    AgentID,
    EnvConfigDict,
    EnvCreator,
    EnvType,
    ModelConfigDict,
    ModelGradients,
    ModelWeights,
    MultiAgentPolicyConfigDict,
    PartialTrainerConfigDict,
    PolicyID,
    PolicyState,
    SampleBatchType,
    T,
)
from ray.util.debug import log_once, disable_log_once_globally, enable_periodic_logging
from ray.util.iter import ParallelIteratorWorker

if TYPE_CHECKING:
    from ray.rllib.evaluation.episode import Episode
    from ray.rllib.evaluation.observation_function import ObservationFunction
    from ray.rllib.agents.callbacks import DefaultCallbacks  # noqa

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


def _update_env_seed_if_necessary(
    env: EnvType, seed: int, worker_idx: int, vector_idx: int
):
    """Set a deterministic random seed on environment.

    NOTE: this may not work with remote environments (issue #18154).
    """
    if not seed:
        return

    # A single RL job is unlikely to have more than 10K
    # rollout workers.
    max_num_envs_per_workers: int = 1000
    assert (
        worker_idx < max_num_envs_per_workers
    ), "Too many envs per worker. Random seeds may collide."
    computed_seed: int = worker_idx * max_num_envs_per_workers + vector_idx + seed

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
        >>> import gym
        >>> from ray.rllib.evaluation.rollout_worker import RolloutWorker
        >>> from ray.rllib.agents.pg.pg_tf_policy import PGTFPolicy
        >>> worker = RolloutWorker( # doctest: +SKIP
        ...   env_creator=lambda _: gym.make("CartPole-v0"), # doctest: +SKIP
        ...   policy_spec=PGTFPolicy) # doctest: +SKIP
        >>> print(worker.sample()) # doctest: +SKIP
        SampleBatch({
            "obs": [[...]], "actions": [[...]], "rewards": [[...]],
            "dones": [[...]], "new_obs": [[...]]})
        >>> # Creating a multi-agent rollout worker
        >>> from gym.spaces import Discrete, Box
        >>> import random
        >>> MultiAgentTrafficGrid = ... # doctest: +SKIP
        >>> worker = RolloutWorker( # doctest: +SKIP
        ...   env_creator=lambda _: MultiAgentTrafficGrid(num_cars=25),
        ...   policy_spec={ # doctest: +SKIP
        ...       # Use an ensemble of two policies for car agents
        ...       "car_policy1": # doctest: +SKIP
        ...         (PGTFPolicy, Box(...), Discrete(...), {"gamma": 0.99}),
        ...       "car_policy2": # doctest: +SKIP
        ...         (PGTFPolicy, Box(...), Discrete(...), {"gamma": 0.95}),
        ...       # Use a single shared policy for all traffic lights
        ...       "traffic_light_policy":
        ...         (PGTFPolicy, Box(...), Discrete(...), {}),
        ...   },
        ...   policy_mapping_fn=lambda agent_id, episode, **kwargs:
        ...     random.choice(["car_policy1", "car_policy2"])
        ...     if agent_id.startswith("car_") else "traffic_light_policy")
        >>> print(worker.sample()) # doctest: +SKIP
        MultiAgentBatch({
            "car_policy1": SampleBatch(...),
            "car_policy2": SampleBatch(...),
            "traffic_light_policy": SampleBatch(...)})
    """

    @DeveloperAPI
    @classmethod
    def as_remote(
        cls,
        num_cpus: Optional[int] = None,
        num_gpus: Optional[Union[int, float]] = None,
        memory: Optional[int] = None,
        object_store_memory: Optional[int] = None,
        resources: Optional[dict] = None,
    ) -> type:
        """Returns RolloutWorker class as a `@ray.remote using given options`.

        The returned class can then be used to instantiate ray actors.

        Args:
            num_cpus: The number of CPUs to allocate for the remote actor.
            num_gpus: The number of GPUs to allocate for the remote actor.
                This could be a fraction as well.
            memory: The heap memory request for the remote actor.
            object_store_memory: The object store memory for the remote actor.
            resources: The default custom resources to allocate for the remote
                actor.

        Returns:
            The `@ray.remote` decorated RolloutWorker class.
        """
        return ray.remote(
            num_cpus=num_cpus,
            num_gpus=num_gpus,
            memory=memory,
            object_store_memory=object_store_memory,
            resources=resources,
        )(cls)

    @DeveloperAPI
    def __init__(
        self,
        *,
        env_creator: EnvCreator,
        validate_env: Optional[Callable[[EnvType, EnvContext], None]] = None,
        policy_spec: Optional[Union[type, Dict[PolicyID, PolicySpec]]] = None,
        policy_mapping_fn: Optional[Callable[[AgentID, "Episode"], PolicyID]] = None,
        policies_to_train: Union[
            Container[PolicyID], Callable[[PolicyID, SampleBatchType], bool]
        ] = None,
        tf_session_creator: Optional[Callable[[], "tf1.Session"]] = None,
        rollout_fragment_length: int = 100,
        count_steps_by: str = "env_steps",
        batch_mode: str = "truncate_episodes",
        episode_horizon: Optional[int] = None,
        preprocessor_pref: str = "deepmind",
        sample_async: bool = False,
        compress_observations: bool = False,
        num_envs: int = 1,
        observation_fn: Optional["ObservationFunction"] = None,
        observation_filter: str = "NoFilter",
        clip_rewards: Optional[Union[bool, float]] = None,
        normalize_actions: bool = True,
        clip_actions: bool = False,
        env_config: Optional[EnvConfigDict] = None,
        model_config: Optional[ModelConfigDict] = None,
        policy_config: Optional[PartialTrainerConfigDict] = None,
        worker_index: int = 0,
        num_workers: int = 0,
        recreated_worker: bool = False,
        record_env: Union[bool, str] = False,
        log_dir: Optional[str] = None,
        log_level: Optional[str] = None,
        callbacks: Type["DefaultCallbacks"] = None,
        input_creator: Callable[
            [IOContext], InputReader
        ] = lambda ioctx: ioctx.default_sampler_input(),
        input_evaluation: List[str] = frozenset([]),
        output_creator: Callable[
            [IOContext], OutputWriter
        ] = lambda ioctx: NoopOutput(),
        remote_worker_envs: bool = False,
        remote_env_batch_wait_ms: int = 0,
        soft_horizon: bool = False,
        no_done_at_end: bool = False,
        seed: int = None,
        extra_python_environs: Optional[dict] = None,
        fake_sampler: bool = False,
        spaces: Optional[Dict[PolicyID, Tuple[Space, Space]]] = None,
        policy=None,
        monitor_path=None,
        disable_env_checking=False,
    ):
        """Initializes a RolloutWorker instance.

        Args:
            env_creator: Function that returns a gym.Env given an EnvContext
                wrapped configuration.
            validate_env: Optional callable to validate the generated
                environment (only on worker=0).
            policy_spec: The MultiAgentPolicyConfigDict mapping policy IDs
                (str) to PolicySpec's or a single policy class to use.
                If a dict is specified, then we are in multi-agent mode and a
                policy_mapping_fn can also be set (if not, will map all agents
                to DEFAULT_POLICY_ID).
            policy_mapping_fn: A callable that maps agent ids to policy ids in
                multi-agent mode. This function will be called each time a new
                agent appears in an episode, to bind that agent to a policy
                for the duration of the episode. If not provided, will map all
                agents to DEFAULT_POLICY_ID.
            policies_to_train: Optional container of policies to train (None
                for all policies), or a callable taking PolicyID and
                SampleBatchType and returning a bool (trainable or not?).
            tf_session_creator: A function that returns a TF session.
                This is optional and only useful with TFPolicy.
            rollout_fragment_length: The target number of steps
                (maesured in `count_steps_by`) to include in each sample
                batch returned from this worker.
            count_steps_by: The unit in which to count fragment
                lengths. One of env_steps or agent_steps.
            batch_mode: One of the following batch modes:
                - "truncate_episodes": Each call to sample() will return a
                batch of at most `rollout_fragment_length * num_envs` in size.
                The batch will be exactly `rollout_fragment_length * num_envs`
                in size if postprocessing does not change batch sizes. Episodes
                may be truncated in order to meet this size requirement.
                - "complete_episodes": Each call to sample() will return a
                batch of at least `rollout_fragment_length * num_envs` in
                size. Episodes will not be truncated, but multiple episodes
                may be packed within one batch to meet the batch size. Note
                that when `num_envs > 1`, episode steps will be buffered
                until the episode completes, and hence batches may contain
                significant amounts of off-policy data.
            episode_horizon: Horizon at which to stop episodes (even if the
                environment itself has not retured a "done" signal).
            preprocessor_pref: Whether to use RLlib preprocessors
                ("rllib") or deepmind ("deepmind"), when applicable.
            sample_async: Whether to compute samples asynchronously in
                the background, which improves throughput but can cause samples
                to be slightly off-policy.
            compress_observations: If true, compress the observations.
                They can be decompressed with rllib/utils/compression.
            num_envs: If more than one, will create multiple envs
                and vectorize the computation of actions. This has no effect if
                if the env already implements VectorEnv.
            observation_fn: Optional multi-agent observation function.
            observation_filter: Name of observation filter to use.
            clip_rewards: True for clipping rewards to [-1.0, 1.0] prior
                to experience postprocessing. None: Clip for Atari only.
                float: Clip to [-clip_rewards; +clip_rewards].
            normalize_actions: Whether to normalize actions to the
                action space's bounds.
            clip_actions: Whether to clip action values to the range
                specified by the policy action space.
            env_config: Config to pass to the env creator.
            model_config: Config to use when creating the policy model.
            policy_config: Config to pass to the
                policy. In the multi-agent case, this config will be merged
                with the per-policy configs specified by `policy_spec`.
            worker_index: For remote workers, this should be set to a
                non-zero and unique value. This index is passed to created envs
                through EnvContext so that envs can be configured per worker.
            num_workers: For remote workers, how many workers altogether
                have been created?
            recreated_worker: Whether this worker is a recreated one. Workers are
                recreated by a Trainer (via WorkerSet) in case
                `recreate_failed_workers=True` and one of the original workers (or an
                already recreated one) has failed. They don't differ from original
                workers other than the value of this flag (`self.recreated_worker`).
            record_env: Write out episode stats and videos
                using gym.wrappers.Monitor to this directory if specified. If
                True, use the default output dir in ~/ray_results/.... If
                False, do not record anything.
            log_dir: Directory where logs can be placed.
            log_level: Set the root log level on creation.
            callbacks: Custom sub-class of
                DefaultCallbacks for training/policy/rollout-worker callbacks.
            input_creator: Function that returns an InputReader object for
                loading previous generated experiences.
            input_evaluation: How to evaluate the policy
                performance. This only makes sense to set when the input is
                reading offline data. The possible values include:
                - "is": the step-wise importance sampling estimator.
                - "wis": the weighted step-wise is estimator.
                - "simulation": run the environment in the background, but
                use this data for evaluation only and never for learning.
            output_creator: Function that returns an OutputWriter object for
                saving generated experiences.
            remote_worker_envs: If using num_envs_per_worker > 1,
                whether to create those new envs in remote processes instead of
                in the current process. This adds overheads, but can make sense
                if your envs are expensive to step/reset (e.g., for StarCraft).
                Use this cautiously, overheads are significant!
            remote_env_batch_wait_ms: Timeout that remote workers
                are waiting when polling environments. 0 (continue when at
                least one env is ready) is a reasonable default, but optimal
                value could be obtained by measuring your environment
                step / reset and model inference perf.
            soft_horizon: Calculate rewards but don't reset the
                environment when the horizon is hit.
            no_done_at_end: Ignore the done=True at the end of the
                episode and instead record done=False.
            seed: Set the seed of both np and tf to this value to
                to ensure each remote worker has unique exploration behavior.
            extra_python_environs: Extra python environments need to be set.
            fake_sampler: Use a fake (inf speed) sampler for testing.
            spaces: An optional space dict mapping policy IDs
                to (obs_space, action_space)-tuples. This is used in case no
                Env is created on this RolloutWorker.
            policy: Obsoleted arg. Use `policy_spec` instead.
            monitor_path: Obsoleted arg. Use `record_env` instead.
            disable_env_checking: If True, disables the env checking module that
                validates the properties of the passed environment.
        """

        # Deprecated args.
        if policy is not None:
            deprecation_warning("policy", "policy_spec", error=False)
            policy_spec = policy
        assert (
            policy_spec is not None
        ), "Must provide `policy_spec` when creating RolloutWorker!"

        # Do quick translation into MultiAgentPolicyConfigDict.
        if not isinstance(policy_spec, dict):
            policy_spec = {DEFAULT_POLICY_ID: PolicySpec(policy_class=policy_spec)}
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

        policy_config = policy_config or {}
        if (
            tf1
            and policy_config.get("framework") in ["tf2", "tfe"]
            # This eager check is necessary for certain all-framework tests
            # that use tf's eager_mode() context generator.
            and not tf1.executing_eagerly()
        ):
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
            remote=remote_worker_envs,
            recreated_worker=recreated_worker,
        )
        self.env_context = env_context
        self.policy_config: PartialTrainerConfigDict = policy_config
        if callbacks:
            self.callbacks: "DefaultCallbacks" = callbacks()
        else:
            from ray.rllib.agents.callbacks import DefaultCallbacks  # noqa

            self.callbacks: DefaultCallbacks = DefaultCallbacks()
        self.worker_index: int = worker_index
        self.num_workers: int = num_workers
        self.recreated_worker: bool = recreated_worker
        model_config: ModelConfigDict = (
            model_config or self.policy_config.get("model") or {}
        )

        # Default policy mapping fn is to always return DEFAULT_POLICY_ID,
        # independent on the agent ID and the episode passed in.
        self.policy_mapping_fn = (
            lambda agent_id, episode, worker, **kwargs: DEFAULT_POLICY_ID
        )
        # If provided, set it here.
        self.set_policy_mapping_fn(policy_mapping_fn)

        self.env_creator: EnvCreator = env_creator
        self.rollout_fragment_length: int = rollout_fragment_length * num_envs
        self.count_steps_by: str = count_steps_by
        self.batch_mode: str = batch_mode
        self.compress_observations: bool = compress_observations
        self.preprocessing_enabled: bool = (
            False if policy_config.get("_disable_preprocessor_api") else True
        )
        self.observation_filter = observation_filter
        self.last_batch: Optional[SampleBatchType] = None
        self.global_vars: Optional[dict] = None
        self.fake_sampler: bool = fake_sampler
        self._disable_env_checking: bool = disable_env_checking

        # Update the global seed for numpy/random/tf-eager/torch if we are not
        # the local worker, otherwise, this was already done in the Trainer
        # object itself.
        if self.worker_index > 0:
            update_global_seed_if_necessary(policy_config.get("framework"), seed)

        # A single environment provided by the user (via config.env). This may
        # also remain None.
        # 1) Create the env using the user provided env_creator. This may
        #    return a gym.Env (incl. MultiAgentEnv), an already vectorized
        #    VectorEnv, BaseEnv, ExternalEnv, or an ActorHandle (remote env).
        # 2) Wrap - if applicable - with Atari/recording/rendering wrappers.
        # 3) Seed the env, if necessary.
        # 4) Vectorize the existing single env by creating more clones of
        #    this env and wrapping it with the RLlib BaseEnv class.
        self.env = self.make_sub_env_fn = None

        # Create a (single) env for this worker.
        if not (
            worker_index == 0
            and num_workers > 0
            and not policy_config.get("create_env_on_driver")
        ):
            # Run the `env_creator` function passing the EnvContext.
            self.env = env_creator(copy.deepcopy(self.env_context))

        if self.env is not None:
            # Validate environment (general validation function).
            if not self._disable_env_checking:
                logger.warning(
                    "We've added a module for checking environments that "
                    "are used in experiments. It will cause your "
                    "environment to fail if your environment is not set up"
                    "correctly. You can disable check env by setting "
                    "`disable_env_checking` to True in your experiment config "
                    "dictionary. You can run the environment checking module "
                    "standalone by calling ray.rllib.utils.check_env(env)."
                )
                check_env(self.env)
            # Custom validation function given, typically a function attribute of the
            # algorithm trainer.
            if validate_env is not None:
                validate_env(self.env, self.env_context)
            # We can't auto-wrap a BaseEnv.
            if isinstance(self.env, (BaseEnv, ray.actor.ActorHandle)):

                def wrap(env):
                    return env

            # Atari type env and "deepmind" preprocessor pref.
            elif (
                is_atari(self.env)
                and not model_config.get("custom_preprocessor")
                and preprocessor_pref == "deepmind"
            ):

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
                        env, dim=model_config.get("dim"), framestack=use_framestack
                    )
                    env = record_env_wrapper(env, record_env, log_dir, policy_config)
                    return env

            # gym.Env -> Wrap with gym Monitor.
            else:

                def wrap(env):
                    return record_env_wrapper(env, record_env, log_dir, policy_config)

            # Wrap env through the correct wrapper.
            self.env: EnvType = wrap(self.env)
            # Ideally, we would use the same make_sub_env() function below
            # to create self.env, but wrap(env) and self.env has a cyclic
            # dependency on each other right now, so we would settle on
            # duplicating the random seed setting logic for now.
            _update_env_seed_if_necessary(self.env, seed, worker_index, 0)
            # Call custom callback function `on_sub_environment_created`.
            self.callbacks.on_sub_environment_created(
                worker=self,
                sub_environment=self.env,
                env_context=self.env_context,
            )

            self.make_sub_env_fn = self._get_make_sub_env_fn(
                env_creator, env_context, validate_env, wrap, seed
            )

        self.spaces = spaces

        self.policy_dict = _determine_spaces_for_multi_agent_dict(
            policy_spec, self.env, spaces=self.spaces, policy_config=policy_config
        )

        # Set of IDs of those policies, which should be trained. This property
        # is optional and mainly used for backward compatibility.
        self.policies_to_train = policies_to_train
        self.is_policy_to_train: Callable[[PolicyID, SampleBatchType], bool]

        # By default (None), use the set of all policies found in the
        # policy_dict.
        if self.policies_to_train is None:
            self.policies_to_train = set(self.policy_dict.keys())

        self.set_is_policy_to_train(self.policies_to_train)

        self.policy_map: PolicyMap = None
        self.preprocessors: Dict[PolicyID, Preprocessor] = None

        # Check available number of GPUs.
        num_gpus = (
            policy_config.get("num_gpus", 0)
            if self.worker_index == 0
            else policy_config.get("num_gpus_per_worker", 0)
        )
        # Error if we don't find enough GPUs.
        if (
            ray.is_initialized()
            and ray.worker._mode() != ray.worker.LOCAL_MODE
            and not policy_config.get("_fake_gpus")
        ):

            devices = []
            if policy_config.get("framework") in ["tf2", "tf", "tfe"]:
                devices = get_tf_gpu_devices()
            elif policy_config.get("framework") == "torch":
                devices = list(range(torch.cuda.device_count()))

            if len(devices) < num_gpus:
                raise RuntimeError(
                    ERR_MSG_NO_GPUS.format(len(devices), devices) + HOWTO_CHANGE_CONFIG
                )
        # Warn, if running in local-mode and actual GPUs (not faked) are
        # requested.
        elif (
            ray.is_initialized()
            and ray.worker._mode() == ray.worker.LOCAL_MODE
            and num_gpus > 0
            and not policy_config.get("_fake_gpus")
        ):
            logger.warning(
                "You are running ray with `local_mode=True`, but have "
                f"configured {num_gpus} GPUs to be used! In local mode, "
                f"Policies are placed on the CPU and the `num_gpus` setting "
                f"is ignored."
            )

        self._build_policy_map(
            self.policy_dict,
            policy_config,
            session_creator=tf_session_creator,
            seed=seed,
        )

        # Update Policy's view requirements from Model, only if Policy directly
        # inherited from base `Policy` class. At this point here, the Policy
        # must have it's Model (if any) defined and ready to output an initial
        # state.
        for pol in self.policy_map.values():
            if not pol._model_init_state_automatically_added:
                pol._update_model_view_requirements_from_init_state()

        self.multiagent: bool = set(self.policy_map.keys()) != {DEFAULT_POLICY_ID}
        if self.multiagent and self.env is not None:
            if not isinstance(
                self.env,
                (BaseEnv, ExternalMultiAgentEnv, MultiAgentEnv, ray.actor.ActorHandle),
            ):
                raise ValueError(
                    f"Have multiple policies {self.policy_map}, but the "
                    f"env {self.env} is not a subclass of BaseEnv, "
                    f"MultiAgentEnv, ActorHandle, or ExternalMultiAgentEnv!"
                )

        self.filters: Dict[PolicyID, Filter] = {}
        for (policy_id, policy) in self.policy_map.items():
            filter_shape = tree.map_structure(
                lambda s: (
                    None
                    if isinstance(s, (Discrete, MultiDiscrete))  # noqa
                    else np.array(s.shape)
                ),
                policy.observation_space_struct,
            )
            self.filters[policy_id] = get_filter(self.observation_filter, filter_shape)

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
            self.async_env: BaseEnv = convert_to_base_env(
                self.env,
                make_env=self.make_sub_env_fn,
                num_envs=num_envs,
                remote_envs=remote_worker_envs,
                remote_env_batch_wait_ms=remote_env_batch_wait_ms,
                worker=self,
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
            raise ValueError("Unsupported batch mode: {}".format(self.batch_mode))

        # Create the IOContext for this worker.
        self.io_context: IOContext = IOContext(
            log_dir, policy_config, worker_index, self
        )
        self.reward_estimators: List[OffPolicyEstimator] = []
        for method in input_evaluation:
            if method == "simulation":
                logger.warning(
                    "Requested 'simulation' input evaluation method: "
                    "will discard all sampler outputs and keep only metrics."
                )
                sample_async = True
            elif method == "is":
                ise = ImportanceSamplingEstimator.create_from_io_context(
                    self.io_context
                )
                self.reward_estimators.append(ise)
            elif method == "wis":
                wise = WeightedImportanceSamplingEstimator.create_from_io_context(
                    self.io_context
                )
                self.reward_estimators.append(wise)
            else:
                raise ValueError("Unknown evaluation method: {}".format(method))

        render = False
        if policy_config.get("render_env") is True and (
            num_workers == 0 or worker_index == 1
        ):
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
                self.async_env, self.env, self.policy_map
            )
        )

    @DeveloperAPI
    def sample(self) -> SampleBatchType:
        """Returns a batch of experience sampled from this worker.

        This method must be implemented by subclasses.

        Returns:
            A columnar batch of experiences (e.g., tensors).

        Examples:
            >>> import gym
            >>> from ray.rllib.evaluation.rollout_worker import RolloutWorker
            >>> from ray.rllib.agents.pg.pg_tf_policy import PGTFPolicy
            >>> worker = RolloutWorker( # doctest: +SKIP
            ...   env_creator=lambda _: gym.make("CartPole-v0"), # doctest: +SKIP
            ...   policy_spec=PGTFPolicy) # doctest: +SKIP
            >>> print(worker.sample()) # doctest: +SKIP
            SampleBatch({"obs": [...], "action": [...], ...})
        """

        if self.fake_sampler and self.last_batch is not None:
            return self.last_batch
        elif self.input_reader is None:
            raise ValueError(
                "RolloutWorker has no `input_reader` object! "
                "Cannot call `sample()`. You can try setting "
                "`create_env_on_driver` to True."
            )

        if log_once("sample_start"):
            logger.info(
                "Generating sample batch of size {}".format(
                    self.rollout_fragment_length
                )
            )

        batches = [self.input_reader.next()]
        steps_so_far = (
            batches[0].count
            if self.count_steps_by == "env_steps"
            else batches[0].agent_steps()
        )

        # In truncate_episodes mode, never pull more than 1 batch per env.
        # This avoids over-running the target batch size.
        if self.batch_mode == "truncate_episodes":
            max_batches = self.num_envs
        else:
            max_batches = float("inf")

        while (
            steps_so_far < self.rollout_fragment_length and len(batches) < max_batches
        ):
            batch = self.input_reader.next()
            steps_so_far += (
                batch.count
                if self.count_steps_by == "env_steps"
                else batch.agent_steps()
            )
            batches.append(batch)
        batch = batches[0].concat_samples(batches) if len(batches) > 1 else batches[0]

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
            logger.info("Completed sample batch:\n\n{}\n".format(summarize(batch)))

        if self.compress_observations:
            batch.compress(bulk=self.compress_observations == "bulk")

        if self.fake_sampler:
            self.last_batch = batch
        return batch

    @DeveloperAPI
    @ray.method(num_returns=2)
    def sample_with_count(self) -> Tuple[SampleBatchType, int]:
        """Same as sample() but returns the count as a separate value.

        Returns:
            A columnar batch of experiences (e.g., tensors) and the
                size of the collected batch.

        Examples:
            >>> import gym
            >>> from ray.rllib.evaluation.rollout_worker import RolloutWorker
            >>> from ray.rllib.agents.pg.pg_tf_policy import PGTFPolicy
            >>> worker = RolloutWorker( # doctest: +SKIP
            ...   env_creator=lambda _: gym.make("CartPole-v0"), # doctest: +SKIP
            ...   policy_spec=PGTFPolicy) # doctest: +SKIP
            >>> print(worker.sample_with_count()) # doctest: +SKIP
            (SampleBatch({"obs": [...], "action": [...], ...}), 3)
        """
        batch = self.sample()
        return batch, batch.count

    @DeveloperAPI
    def learn_on_batch(self, samples: SampleBatchType) -> Dict:
        """Update policies based on the given batch.

        This is the equivalent to apply_gradients(compute_gradients(samples)),
        but can be optimized to avoid pulling gradients into CPU memory.

        Args:
            samples: The SampleBatch or MultiAgentBatch to learn on.

        Returns:
            Dictionary of extra metadata from compute_gradients().

        Examples:
            >>> import gym
            >>> from ray.rllib.evaluation.rollout_worker import RolloutWorker
            >>> from ray.rllib.agents.pg.pg_tf_policy import PGTFPolicy
            >>> worker = RolloutWorker( # doctest: +SKIP
            ...   env_creator=lambda _: gym.make("CartPole-v0"), # doctest: +SKIP
            ...   policy_spec=PGTFPolicy) # doctest: +SKIP
            >>> batch = worker.sample() # doctest: +SKIP
            >>> info = worker.learn_on_batch(samples) # doctest: +SKIP
        """
        if log_once("learn_on_batch"):
            logger.info(
                "Training on concatenated sample batches:\n\n{}\n".format(
                    summarize(samples)
                )
            )

        info_out = {}
        if isinstance(samples, MultiAgentBatch):
            builders = {}
            to_fetch = {}
            for pid, batch in samples.policy_batches.items():
                if not self.is_policy_to_train(pid, samples):
                    continue
                # Decompress SampleBatch, in case some columns are compressed.
                batch.decompress_if_needed()
                policy = self.policy_map[pid]
                tf_session = policy.get_session()
                if tf_session and hasattr(policy, "_build_learn_on_batch"):
                    builders[pid] = TFRunBuilder(tf_session, "learn_on_batch")
                    to_fetch[pid] = policy._build_learn_on_batch(builders[pid], batch)
                else:
                    info_out[pid] = policy.learn_on_batch(batch)
            info_out.update({pid: builders[pid].get(v) for pid, v in to_fetch.items()})
        else:
            if self.is_policy_to_train(DEFAULT_POLICY_ID, samples):
                info_out = {
                    DEFAULT_POLICY_ID: self.policy_map[
                        DEFAULT_POLICY_ID
                    ].learn_on_batch(samples)
                }
        if log_once("learn_out"):
            logger.debug("Training out:\n\n{}\n".format(summarize(info_out)))
        return info_out

    def sample_and_learn(
        self,
        expected_batch_size: int,
        num_sgd_iter: int,
        sgd_minibatch_size: str,
        standardize_fields: List[str],
    ) -> Tuple[dict, int]:
        """Sample and batch and learn on it.

        This is typically used in combination with distributed allreduce.

        Args:
            expected_batch_size: Expected number of samples to learn on.
            num_sgd_iter: Number of SGD iterations.
            sgd_minibatch_size: SGD minibatch size.
            standardize_fields: List of sample fields to normalize.

        Returns:
            A tuple consisting of a dictionary of extra metadata returned from
                the policies' `learn_on_batch()` and the number of samples
                learned on.
        """
        batch = self.sample()
        assert batch.count == expected_batch_size, (
            "Batch size possibly out of sync between workers, expected:",
            expected_batch_size,
            "got:",
            batch.count,
        )
        logger.info(
            "Executing distributed minibatch SGD "
            "with epoch size {}, minibatch size {}".format(
                batch.count, sgd_minibatch_size
            )
        )
        info = do_minibatch_sgd(
            batch,
            self.policy_map,
            self,
            num_sgd_iter,
            sgd_minibatch_size,
            standardize_fields,
        )
        return info, batch.count

    @DeveloperAPI
    def compute_gradients(
        self,
        samples: SampleBatchType,
        single_agent: bool = None,
    ) -> Tuple[ModelGradients, dict]:
        """Returns a gradient computed w.r.t the specified samples.

        Uses the Policy's/ies' compute_gradients method(s) to perform the
        calculations. Skips policies that are not trainable as per
        `self.is_policy_to_train()`.

        Args:
            samples: The SampleBatch or MultiAgentBatch to compute gradients
                for using this worker's trainable policies.

        Returns:
            In the single-agent case, a tuple consisting of ModelGradients and
            info dict of the worker's policy.
            In the multi-agent case, a tuple consisting of a dict mapping
            PolicyID to ModelGradients and a dict mapping PolicyID to extra
            metadata info.
            Note that the first return value (grads) can be applied as is to a
            compatible worker using the worker's `apply_gradients()` method.

        Examples:
            >>> import gym
            >>> from ray.rllib.evaluation.rollout_worker import RolloutWorker
            >>> from ray.rllib.agents.pg.pg_tf_policy import PGTFPolicy
            >>> worker = RolloutWorker( # doctest: +SKIP
            ...   env_creator=lambda _: gym.make("CartPole-v0"), # doctest: +SKIP
            ...   policy_spec=PGTFPolicy) # doctest: +SKIP
            >>> batch = worker.sample() # doctest: +SKIP
            >>> grads, info = worker.compute_gradients(samples) # doctest: +SKIP
        """
        if log_once("compute_gradients"):
            logger.info("Compute gradients on:\n\n{}\n".format(summarize(samples)))

        # Backward compatiblity for A2C: Single-agent only (ComputeGradients execution
        # op must not return multi-agent dict b/c of A2C's `.batch()` in the execution
        # plan; this would "batch" over the "default_policy" keys instead of the data).
        if single_agent is True:
            # SampleBatch -> Calculate gradients for the default policy.
            grad_out, info_out = self.policy_map[DEFAULT_POLICY_ID].compute_gradients(
                samples
            )
            info_out["batch_count"] = samples.count
            return grad_out, info_out

        # Treat everything as is multi-agent.
        samples = samples.as_multi_agent()

        # Calculate gradients for all policies.
        grad_out, info_out = {}, {}
        if self.policy_config.get("framework") == "tf":
            for pid, batch in samples.policy_batches.items():
                if not self.is_policy_to_train(pid, samples):
                    continue
                policy = self.policy_map[pid]
                builder = TFRunBuilder(policy.get_session(), "compute_gradients")
                grad_out[pid], info_out[pid] = policy._build_compute_gradients(
                    builder, batch
                )
            grad_out = {k: builder.get(v) for k, v in grad_out.items()}
            info_out = {k: builder.get(v) for k, v in info_out.items()}
        else:
            for pid, batch in samples.policy_batches.items():
                if not self.is_policy_to_train(pid, samples):
                    continue
                grad_out[pid], info_out[pid] = self.policy_map[pid].compute_gradients(
                    batch
                )

        info_out["batch_count"] = samples.count
        if log_once("grad_out"):
            logger.info("Compute grad info:\n\n{}\n".format(summarize(info_out)))

        return grad_out, info_out

    @DeveloperAPI
    def apply_gradients(
        self,
        grads: Union[ModelGradients, Dict[PolicyID, ModelGradients]],
    ) -> None:
        """Applies the given gradients to this worker's models.

        Uses the Policy's/ies' apply_gradients method(s) to perform the
        operations.

        Args:
            grads: Single ModelGradients (single-agent case) or a dict
                mapping PolicyIDs to the respective model gradients
                structs.

        Examples:
            >>> import gym
            >>> from ray.rllib.evaluation.rollout_worker import RolloutWorker
            >>> from ray.rllib.agents.pg.pg_tf_policy import PGTFPolicy
            >>> worker = RolloutWorker( # doctest: +SKIP
            ...   env_creator=lambda _: gym.make("CartPole-v0"), # doctest: +SKIP
            ...   policy_spec=PGTFPolicy) # doctest: +SKIP
            >>> samples = worker.sample() # doctest: +SKIP
            >>> grads, info = worker.compute_gradients(samples) # doctest: +SKIP
            >>> worker.apply_gradients(grads) # doctest: +SKIP
        """
        if log_once("apply_gradients"):
            logger.info("Apply gradients:\n\n{}\n".format(summarize(grads)))
        # Grads is a dict (mapping PolicyIDs to ModelGradients).
        # Multi-agent case.
        if isinstance(grads, dict):
            for pid, g in grads.items():
                if self.is_policy_to_train(pid, None):
                    self.policy_map[pid].apply_gradients(g)
        # Grads is a ModelGradients type. Single-agent case.
        elif self.is_policy_to_train(DEFAULT_POLICY_ID, None):
            self.policy_map[DEFAULT_POLICY_ID].apply_gradients(grads)

    @DeveloperAPI
    def get_metrics(self) -> List[Union[RolloutMetrics, OffPolicyEstimate]]:
        """Returns the thus-far collected metrics from this worker's rollouts.

        Returns:
             List of RolloutMetrics and/or OffPolicyEstimate objects
             collected thus-far.
        """

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
    def foreach_env(self, func: Callable[[EnvType], T]) -> List[T]:
        """Calls the given function with each sub-environment as arg.

        Args:
            func: The function to call for each underlying
                sub-environment (as only arg).

        Returns:
             The list of return values of all calls to `func([env])`.
        """

        if self.async_env is None:
            return []

        envs = self.async_env.get_sub_environments()
        # Empty list (not implemented): Call function directly on the
        # BaseEnv.
        if not envs:
            return [func(self.async_env)]
        # Call function on all underlying (vectorized) sub environments.
        else:
            return [func(e) for e in envs]

    @DeveloperAPI
    def foreach_env_with_context(
        self, func: Callable[[EnvType, EnvContext], T]
    ) -> List[T]:
        """Calls given function with each sub-env plus env_ctx as args.

        Args:
            func: The function to call for each underlying
                sub-environment and its EnvContext (as the args).

        Returns:
             The list of return values of all calls to `func([env, ctx])`.
        """

        if self.async_env is None:
            return []

        envs = self.async_env.get_sub_environments()
        # Empty list (not implemented): Call function directly on the
        # BaseEnv.
        if not envs:
            return [func(self.async_env, self.env_context)]
        # Call function on all underlying (vectorized) sub environments.
        else:
            ret = []
            for i, e in enumerate(envs):
                ctx = self.env_context.copy_with_overrides(vector_index=i)
                ret.append(func(e, ctx))
            return ret

    @DeveloperAPI
    def get_policy(self, policy_id: PolicyID = DEFAULT_POLICY_ID) -> Optional[Policy]:
        """Return policy for the specified id, or None.

        Args:
            policy_id: ID of the policy to return. None for DEFAULT_POLICY_ID
                (in the single agent case).

        Returns:
            The policy under the given ID (or None if not found).
        """
        return self.policy_map.get(policy_id)

    @DeveloperAPI
    def add_policy(
        self,
        *,
        policy_id: PolicyID,
        policy_cls: Type[Policy],
        observation_space: Optional[Space] = None,
        action_space: Optional[Space] = None,
        config: Optional[PartialTrainerConfigDict] = None,
        policy_state: Optional[PolicyState] = None,
        policy_mapping_fn: Optional[Callable[[AgentID, "Episode"], PolicyID]] = None,
        policies_to_train: Optional[
            Union[Container[PolicyID], Callable[[PolicyID, SampleBatchType], bool]]
        ] = None,
    ) -> Policy:
        """Adds a new policy to this RolloutWorker.

        Args:
            policy_id: ID of the policy to add.
            policy_cls: The Policy class to use for constructing the new
                Policy.
            observation_space: The observation space of the policy to add.
            action_space: The action space of the policy to add.
            config: The config overrides for the policy to add.
            policy_state: Optional state dict to apply to the new
                policy instance, right after its construction.
            policy_mapping_fn: An optional (updated) policy mapping function
                to use from here on. Note that already ongoing episodes will
                not change their mapping but will use the old mapping till
                the end of the episode.
            policies_to_train: An optional container of policy IDs to be
                trained or a callable taking PolicyID and - optionally -
                SampleBatchType and returning a bool (trainable or not?).
                If None, will keep the existing setup in place.
                Policies, whose IDs are not in the list (or for which the
                callable returns False) will not be updated.

        Returns:
            The newly added policy.

        Raises:
            KeyError: If the given `policy_id` already exists in this worker's
                PolicyMap.
        """
        if policy_id in self.policy_map:
            raise KeyError(f"Policy ID '{policy_id}' already in policy map!")
        policy_dict_to_add = _determine_spaces_for_multi_agent_dict(
            {
                policy_id: PolicySpec(
                    policy_cls, observation_space, action_space, config or {}
                )
            },
            self.env,
            spaces=self.spaces,
            policy_config=self.policy_config,
        )
        self.policy_dict.update(policy_dict_to_add)
        self._build_policy_map(
            policy_dict_to_add, self.policy_config, seed=self.policy_config.get("seed")
        )
        new_policy = self.policy_map[policy_id]
        # Set the state of the newly created policy.
        if policy_state:
            new_policy.set_state(policy_state)

        self.filters[policy_id] = get_filter(
            self.observation_filter, new_policy.observation_space.shape
        )

        self.set_policy_mapping_fn(policy_mapping_fn)
        if policies_to_train is not None:
            self.set_is_policy_to_train(policies_to_train)

        return new_policy

    @DeveloperAPI
    def remove_policy(
        self,
        *,
        policy_id: PolicyID = DEFAULT_POLICY_ID,
        policy_mapping_fn: Optional[Callable[[AgentID], PolicyID]] = None,
        policies_to_train: Optional[
            Union[Container[PolicyID], Callable[[PolicyID, SampleBatchType], bool]]
        ] = None,
    ) -> None:
        """Removes a policy from this RolloutWorker.

        Args:
            policy_id: ID of the policy to be removed. None for
                DEFAULT_POLICY_ID.
            policy_mapping_fn: An optional (updated) policy mapping function
                to use from here on. Note that already ongoing episodes will
                not change their mapping but will use the old mapping till
                the end of the episode.
            policies_to_train: An optional container of policy IDs to be
                trained or a callable taking PolicyID and - optionally -
                SampleBatchType and returning a bool (trainable or not?).
                If None, will keep the existing setup in place.
                Policies, whose IDs are not in the list (or for which the
                callable returns False) will not be updated.
        """
        if policy_id not in self.policy_map:
            raise ValueError(f"Policy ID '{policy_id}' not in policy map!")
        del self.policy_map[policy_id]
        del self.preprocessors[policy_id]
        self.set_policy_mapping_fn(policy_mapping_fn)
        if policies_to_train is not None:
            self.set_is_policy_to_train(policies_to_train)

    @DeveloperAPI
    def set_policy_mapping_fn(
        self,
        policy_mapping_fn: Optional[Callable[[AgentID, "Episode"], PolicyID]] = None,
    ) -> None:
        """Sets `self.policy_mapping_fn` to a new callable (if provided).

        Args:
            policy_mapping_fn: The new mapping function to use. If None,
                will keep the existing mapping function in place.
        """
        if policy_mapping_fn is not None:
            self.policy_mapping_fn = policy_mapping_fn
            if not callable(self.policy_mapping_fn):
                raise ValueError("`policy_mapping_fn` must be a callable!")

    @DeveloperAPI
    def set_is_policy_to_train(
        self,
        is_policy_to_train: Union[
            Container[PolicyID], Callable[[PolicyID, Optional[SampleBatchType]], bool]
        ],
    ) -> None:
        """Sets `self.is_policy_to_train()` to a new callable.

        Args:
            is_policy_to_train: A container of policy IDs to be
                trained or a callable taking PolicyID and - optionally -
                SampleBatchType and returning a bool (trainable or not?).
                If None, will keep the existing setup in place.
                Policies, whose IDs are not in the list (or for which the
                callable returns False) will not be updated.
        """
        # If container given, construct a simple default callable returning True
        # if the PolicyID is found in the list/set of IDs.
        if not callable(is_policy_to_train):
            assert isinstance(is_policy_to_train, Container), (
                "ERROR: `is_policy_to_train`must be a container or a "
                "callable taking PolicyID and SampleBatch and returning "
                "True|False (trainable or not?)."
            )
            pols = set(is_policy_to_train)

            def is_policy_to_train(pid, batch=None):
                return pid in pols

        self.is_policy_to_train = is_policy_to_train

    @ExperimentalAPI
    def get_policies_to_train(
        self, batch: Optional[SampleBatchType] = None
    ) -> Set[PolicyID]:
        """Returns all policies-to-train, given an optional batch.

        Loops through all policies currently in `self.policy_map` and checks
        the return value of `self.is_policy_to_train(pid, batch)`.

        Args:
            batch: An optional SampleBatchType for the
                `self.is_policy_to_train(pid, [batch]?)` check.

        Returns:
            The set of currently trainable policy IDs, given the optional
            `batch`.
        """
        return {
            pid for pid in self.policy_map.keys() if self.is_policy_to_train(pid, batch)
        }

    @DeveloperAPI
    def for_policy(
        self,
        func: Callable[[Policy, Optional[Any]], T],
        policy_id: Optional[PolicyID] = DEFAULT_POLICY_ID,
        **kwargs,
    ) -> T:
        """Calls the given function with the specified policy as first arg.

        Args:
            func: The function to call with the policy as first arg.
            policy_id: The PolicyID of the policy to call the function with.

        Keyword Args:
            kwargs: Additional kwargs to be passed to the call.

        Returns:
            The return value of the function call.
        """

        return func(self.policy_map[policy_id], **kwargs)

    @DeveloperAPI
    def foreach_policy(
        self, func: Callable[[Policy, PolicyID, Optional[Any]], T], **kwargs
    ) -> List[T]:
        """Calls the given function with each (policy, policy_id) tuple.

        Args:
            func: The function to call with each (policy, policy ID) tuple.

        Keyword Args:
            kwargs: Additional kwargs to be passed to the call.

        Returns:
             The list of return values of all calls to
                `func([policy, pid, **kwargs])`.
        """
        return [func(policy, pid, **kwargs) for pid, policy in self.policy_map.items()]

    @DeveloperAPI
    def foreach_policy_to_train(
        self, func: Callable[[Policy, PolicyID, Optional[Any]], T], **kwargs
    ) -> List[T]:
        """
        Calls the given function with each (policy, policy_id) tuple.

        Only those policies/IDs will be called on, for which
        `self.is_policy_to_train()` returns True.

        Args:
            func: The function to call with each (policy, policy ID) tuple,
                for only those policies that `self.is_policy_to_train`
                returns True.

        Keyword Args:
            kwargs: Additional kwargs to be passed to the call.

        Returns:
            The list of return values of all calls to
            `func([policy, pid, **kwargs])`.
        """
        return [
            func(policy, pid, **kwargs)
            for pid, policy in self.policy_map.items()
            if self.is_policy_to_train(pid, None)
        ]

    @DeveloperAPI
    def sync_filters(self, new_filters: dict) -> None:
        """Changes self's filter to given and rebases any accumulated delta.

        Args:
            new_filters: Filters with new state to update local copy.
        """
        assert all(k in new_filters for k in self.filters)
        for k in self.filters:
            self.filters[k].sync(new_filters[k])

    @DeveloperAPI
    def get_filters(self, flush_after: bool = False) -> Dict:
        """Returns a snapshot of filters.

        Args:
            flush_after: Clears the filter buffer state.

        Returns:
            Dict for serializable filters
        """
        return_filters = {}
        for k, f in self.filters.items():
            return_filters[k] = f.as_serializable()
            if flush_after:
                f.reset_buffer()
        return return_filters

    @DeveloperAPI
    def save(self) -> bytes:
        """Serializes this RolloutWorker's current state and returns it.

        Returns:
            The current state of this RolloutWorker as a serialized, pickled
            byte sequence.
        """
        filters = self.get_filters(flush_after=True)
        state = {}
        policy_specs = {}
        for pid in self.policy_map:
            state[pid] = self.policy_map[pid].get_state()
            policy_specs[pid] = self.policy_map.policy_specs[pid]
        return pickle.dumps(
            {
                "filters": filters,
                "state": state,
                "policy_specs": policy_specs,
            }
        )

    @DeveloperAPI
    def restore(self, objs: bytes) -> None:
        """Restores this RolloutWorker's state from a sequence of bytes.

        Args:
            objs: The byte sequence to restore this worker's state from.

        Examples:
            >>> from ray.rllib.evaluation.rollout_worker import RolloutWorker
            >>> # Create a RolloutWorker.
            >>> worker = ... # doctest: +SKIP
            >>> state = worker.save() # doctest: +SKIP
            >>> new_worker = RolloutWorker(...) # doctest: +SKIP
            >>> new_worker.restore(state) # doctest: +SKIP
        """
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
                        "state. Will not add `{pid}`, but ignore it for now."
                    )
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
    def get_weights(
        self,
        policies: Optional[Container[PolicyID]] = None,
    ) -> Dict[PolicyID, ModelWeights]:
        """Returns each policies' model weights of this worker.

        Args:
            policies: List of PolicyIDs to get the weights from.
                Use None for all policies.

        Returns:
            Dict mapping PolicyIDs to ModelWeights.

        Examples:
            >>> from ray.rllib.evaluation.rollout_worker import RolloutWorker
            >>> # Create a RolloutWorker.
            >>> worker = ... # doctest: +SKIP
            >>> weights = worker.get_weights() # doctest: +SKIP
            >>> print(weights) # doctest: +SKIP
            {"default_policy": {"layer1": array(...), "layer2": ...}}
        """
        if policies is None:
            policies = list(self.policy_map.keys())
        policies = force_list(policies)

        return {
            pid: policy.get_weights()
            for pid, policy in self.policy_map.items()
            if pid in policies
        }

    @DeveloperAPI
    def set_weights(
        self, weights: Dict[PolicyID, ModelWeights], global_vars: Optional[Dict] = None
    ) -> None:
        """Sets each policies' model weights of this worker.

        Args:
            weights: Dict mapping PolicyIDs to the new weights to be used.
            global_vars: An optional global vars dict to set this
                worker to. If None, do not update the global_vars.

        Examples:
            >>> from ray.rllib.evaluation.rollout_worker import RolloutWorker
            >>> # Create a RolloutWorker.
            >>> worker = ... # doctest: +SKIP
            >>> weights = worker.get_weights() # doctest: +SKIP
            >>> # Set `global_vars` (timestep) as well.
            >>> worker.set_weights(weights, {"timestep": 42}) # doctest: +SKIP
        """
        # If per-policy weights are object refs, `ray.get()` them first.
        if weights and isinstance(next(iter(weights.values())), ObjectRef):
            actual_weights = ray.get(list(weights.values()))
            weights = {pid: actual_weights[i] for i, pid in enumerate(weights.keys())}

        for pid, w in weights.items():
            self.policy_map[pid].set_weights(w)
        if global_vars:
            self.set_global_vars(global_vars)

    @DeveloperAPI
    def get_global_vars(self) -> dict:
        """Returns the current global_vars dict of this worker.

        Returns:
            The current global_vars dict of this worker.

        Examples:
            >>> from ray.rllib.evaluation.rollout_worker import RolloutWorker
            >>> # Create a RolloutWorker.
            >>> worker = ... # doctest: +SKIP
            >>> global_vars = worker.get_global_vars() # doctest: +SKIP
            >>> print(global_vars) # doctest: +SKIP
            {"timestep": 424242}
        """
        return self.global_vars

    @DeveloperAPI
    def set_global_vars(self, global_vars: dict) -> None:
        """Updates this worker's and all its policies' global vars.

        Args:
            global_vars: The new global_vars dict.

        Examples:
            >>> worker = ... # doctest: +SKIP
            >>> global_vars = worker.set_global_vars( # doctest: +SKIP
            ...     {"timestep": 4242})
        """
        self.foreach_policy(lambda p, _: p.on_global_var_update(global_vars))
        self.global_vars = global_vars

    @DeveloperAPI
    def stop(self) -> None:
        """Releases all resources used by this RolloutWorker."""

        # If we have an env -> Release its resources.
        if self.env is not None:
            self.async_env.stop()
        # Close all policies' sessions (if tf static graph).
        for policy in self.policy_map.values():
            sess = policy.get_session()
            # Closes the tf session, if any.
            if sess is not None:
                sess.close()

    @DeveloperAPI
    def apply(
        self,
        func: Callable[["RolloutWorker", Optional[Any], Optional[Any]], T],
        *args,
        **kwargs,
    ) -> T:
        """Calls the given function with this rollout worker instance.

        Useful for when the RolloutWorker class has been converted into a
        ActorHandle and the user needs to execute some functionality (e.g.
        add a property) on the underlying policy object.

        Args:
            func: The function to call, with this RolloutWorker as first
                argument, followed by args, and kwargs.
            args: Optional additional args to pass to the function call.
            kwargs: Optional additional kwargs to pass to the function call.

        Returns:
            The return value of the function call.
        """
        return func(self, *args, **kwargs)

    def setup_torch_data_parallel(
        self, url: str, world_rank: int, world_size: int, backend: str
    ) -> None:
        """Join a torch process group for distributed SGD."""

        logger.info(
            "Joining process group, url={}, world_rank={}, "
            "world_size={}, backend={}".format(url, world_rank, world_size, backend)
        )
        torch.distributed.init_process_group(
            backend=backend, init_method=url, rank=world_rank, world_size=world_size
        )

        for pid, policy in self.policy_map.items():
            if not isinstance(policy, TorchPolicy):
                raise ValueError(
                    "This policy does not support torch distributed", policy
                )
            policy.distributed_world_size = world_size

    @DeveloperAPI
    def creation_args(self) -> dict:
        """Returns the kwargs dict used to create this worker."""
        return self._original_kwargs

    @DeveloperAPI
    def get_host(self) -> str:
        """Returns the hostname of the process running this evaluator."""
        return platform.node()

    @DeveloperAPI
    def get_node_ip(self) -> str:
        """Returns the IP address of the node that this worker runs on."""
        return ray.util.get_node_ip_address()

    @DeveloperAPI
    def find_free_port(self) -> int:
        """Finds a free port on the node that this worker runs on."""
        from ray.util.ml_utils.util import find_free_port

        return find_free_port()

    def __del__(self):
        """If this worker is deleted, clears all resources used by it."""

        # In case we have-an AsyncSampler, kill its sampling thread.
        if hasattr(self, "sampler") and isinstance(self.sampler, AsyncSampler):
            self.sampler.shutdown = True

    def _build_policy_map(
        self,
        policy_dict: MultiAgentPolicyConfigDict,
        policy_config: PartialTrainerConfigDict,
        session_creator: Optional[Callable[[], "tf1.Session"]] = None,
        seed: Optional[int] = None,
    ) -> None:
        """Adds the given policy_dict to `self.policy_map`.

        Args:
            policy_dict: The MultiAgentPolicyConfigDict to be added to this
                worker's PolicyMap.
            policy_config: The general policy config to use. May be updated
                by individual policy condig overrides in the given
                multi-agent `policy_dict`.
            session_creator: A callable that creates a tf session
                (if applicable).
            seed: An optional random seed to pass to PolicyMap's
                constructor.
        """
        ma_config = policy_config.get("multiagent", {})

        # If our policy_map does not exist yet, create it here.
        self.policy_map = self.policy_map or PolicyMap(
            worker_index=self.worker_index,
            num_workers=self.num_workers,
            capacity=ma_config.get("policy_map_capacity"),
            path=ma_config.get("policy_map_cache"),
            policy_config=policy_config,
            session_creator=session_creator,
            seed=seed,
        )
        # If our preprocessors dict does not exist yet, create it here.
        self.preprocessors = self.preprocessors or {}

        # Loop through given policy-dict and add each entry to our map.
        for name, (orig_cls, obs_space, act_space, conf) in sorted(policy_dict.items()):
            logger.debug("Creating policy for {}".format(name))
            # Update the general policy_config with the specific config
            # for this particular policy.
            merged_conf = merge_dicts(policy_config, conf or {})
            # Update num_workers and worker_index.
            merged_conf["num_workers"] = self.num_workers
            merged_conf["worker_index"] = self.worker_index
            # Preprocessors.
            if self.preprocessing_enabled:
                preprocessor = ModelCatalog.get_preprocessor_for_space(
                    obs_space, merged_conf.get("model")
                )
                self.preprocessors[name] = preprocessor
                if preprocessor is not None:
                    obs_space = preprocessor.observation_space
            else:
                self.preprocessors[name] = None
            # Create the actual policy object.
            self.policy_map.create_policy(
                name, orig_cls, obs_space, act_space, conf, merged_conf
            )

        if self.worker_index == 0:
            logger.info(f"Built policy map: {self.policy_map}")
            logger.info(f"Built preprocessor map: {self.preprocessors}")

    def _get_make_sub_env_fn(
        self, env_creator, env_context, validate_env, env_wrapper, seed
    ):
        disable_env_checking = self._disable_env_checking

        def _make_sub_env_local(vector_index):
            # Used to created additional environments during environment
            # vectorization.

            # Create the env context (config dict + meta-data) for
            # this particular sub-env within the vectorized one.
            env_ctx = env_context.copy_with_overrides(vector_index=vector_index)
            # Create the sub-env.
            env = env_creator(env_ctx)
            # Validate first.
            if not disable_env_checking:
                logger.warning(
                    "We've added a module for checking environments that "
                    "are used in experiments. It will cause your "
                    "environment to fail if your environment is not set up"
                    "correctly. You can disable check env by setting "
                    "`disable_env_checking` to True in your experiment config "
                    "dictionary. You can run the environment checking module "
                    "standalone by calling ray.rllib.utils.check_env(env)."
                )
                check_env(env)
            # Custom validation function given by user.
            if validate_env is not None:
                validate_env(env, env_ctx)
            # Use our wrapper, defined above.
            env = env_wrapper(env)

            # Make sure a deterministic random seed is set on
            # all the sub-environments if specified.
            _update_env_seed_if_necessary(
                env, seed, env_context.worker_index, vector_index
            )
            return env

        if not env_context.remote:

            def _make_sub_env_remote(vector_index):
                sub_env = _make_sub_env_local(vector_index)
                self.callbacks.on_sub_environment_created(
                    worker=self,
                    sub_environment=sub_env,
                    env_context=env_context.copy_with_overrides(
                        worker_index=env_context.worker_index,
                        vector_index=vector_index,
                        remote=False,
                    ),
                )
                return sub_env

            return _make_sub_env_remote

        else:
            return _make_sub_env_local

    @Deprecated(
        new="Trainer.get_policy().export_model([export_dir], [onnx]?)", error=False
    )
    def export_policy_model(
        self,
        export_dir: str,
        policy_id: PolicyID = DEFAULT_POLICY_ID,
        onnx: Optional[int] = None,
    ):
        self.policy_map[policy_id].export_model(export_dir, onnx=onnx)

    @Deprecated(
        new="Trainer.get_policy().import_model_from_h5([import_file])", error=False
    )
    def import_policy_model_from_h5(
        self, import_file: str, policy_id: PolicyID = DEFAULT_POLICY_ID
    ):
        self.policy_map[policy_id].import_model_from_h5(import_file)

    @Deprecated(
        new="Trainer.get_policy().export_checkpoint([export_dir], [filename]?)",
        error=False,
    )
    def export_policy_checkpoint(
        self,
        export_dir: str,
        filename_prefix: str = "model",
        policy_id: PolicyID = DEFAULT_POLICY_ID,
    ):
        self.policy_map[policy_id].export_checkpoint(export_dir, filename_prefix)

    @Deprecated(new="RolloutWorker.foreach_policy_to_train", error=False)
    def foreach_trainable_policy(self, func, **kwargs):
        return self.foreach_policy_to_train(func, **kwargs)


def _determine_spaces_for_multi_agent_dict(
    multi_agent_dict: MultiAgentPolicyConfigDict,
    env: Optional[EnvType] = None,
    spaces: Optional[Dict[PolicyID, Tuple[Space, Space]]] = None,
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
            env.observation_space, gym.Space
        ):
            env_obs_space = env.observation_space

        if hasattr(env, "action_space") and isinstance(env.action_space, gym.Space):
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
                    "`observation_space` specified in config!"
                )

            multi_agent_dict[pid] = multi_agent_dict[pid]._replace(
                observation_space=obs_space
            )

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
                    "`action_space` specified in config!"
                )
            multi_agent_dict[pid] = multi_agent_dict[pid]._replace(
                action_space=act_space
            )
    return multi_agent_dict

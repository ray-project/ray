from collections import defaultdict
import concurrent
import copy
from datetime import datetime
import functools
import gymnasium as gym
import importlib
import importlib.metadata
import json
import logging
import numpy as np
import os
from packaging import version
import re
import tempfile
import time
from typing import (
    Callable,
    Container,
    DefaultDict,
    Dict,
    List,
    Optional,
    Set,
    Tuple,
    Type,
    TYPE_CHECKING,
    Union,
)

import tree  # pip install dm_tree

import ray
from ray._private.usage.usage_lib import TagKey, record_extra_usage_tag
from ray.actor import ActorHandle
from ray.train import Checkpoint
import ray.cloudpickle as pickle
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.algorithms.registry import ALGORITHMS_CLASS_TO_NAME as ALL_ALGORITHMS
from ray.rllib.connectors.agent.obs_preproc import ObsPreprocessorConnector
from ray.rllib.core import DEFAULT_AGENT_ID, DEFAULT_MODULE_ID
from ray.rllib.core.columns import Columns
from ray.rllib.core.rl_module.marl_module import MultiAgentRLModuleSpec
from ray.rllib.core.rl_module.rl_module import RLModule, SingleAgentRLModuleSpec
from ray.rllib.env.env_context import EnvContext
from ray.rllib.env.env_runner import EnvRunner
from ray.rllib.env.env_runner_group import EnvRunnerGroup
from ray.rllib.env.utils import _gym_env_creator
from ray.rllib.evaluation.episode import Episode
from ray.rllib.evaluation.metrics import (
    collect_episodes,
    summarize_episodes,
)
from ray.rllib.execution.rollout_ops import synchronous_parallel_sample
from ray.rllib.offline import get_dataset_and_shards
from ray.rllib.offline.estimators import (
    OffPolicyEstimator,
    ImportanceSampling,
    WeightedImportanceSampling,
    DirectMethod,
    DoublyRobust,
)
from ray.rllib.offline.offline_evaluator import OfflineEvaluator
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID, SampleBatch
from ray.rllib.utils import deep_update, FilterManager
from ray.rllib.utils.annotations import (
    DeveloperAPI,
    ExperimentalAPI,
    OldAPIStack,
    override,
    OverrideToImplementCustomLogic,
    OverrideToImplementCustomLogic_CallToSuperRecommended,
    PublicAPI,
)
from ray.rllib.utils.checkpoints import (
    CHECKPOINT_VERSION,
    CHECKPOINT_VERSION_LEARNER,
    get_checkpoint_info,
    try_import_msgpack,
)
from ray.rllib.utils.debug import update_global_seed_if_necessary
from ray.rllib.utils.deprecation import (
    DEPRECATED_VALUE,
    Deprecated,
    deprecation_warning,
)
from ray.rllib.utils.error import ERR_MSG_INVALID_ENV_DESCRIPTOR, EnvError
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.from_config import from_config
from ray.rllib.utils.metrics import (
    ALL_MODULES,
    ENV_RUNNER_RESULTS,
    ENV_RUNNER_SAMPLING_TIMER,
    EVALUATION_ITERATION_TIMER,
    EVALUATION_RESULTS,
    FAULT_TOLERANCE_STATS,
    LEARNER_RESULTS,
    LEARNER_UPDATE_TIMER,
    NUM_AGENT_STEPS_SAMPLED,
    NUM_AGENT_STEPS_SAMPLED_LIFETIME,
    NUM_AGENT_STEPS_SAMPLED_THIS_ITER,
    NUM_AGENT_STEPS_TRAINED,
    NUM_AGENT_STEPS_TRAINED_LIFETIME,
    NUM_ENV_STEPS_SAMPLED,
    NUM_ENV_STEPS_SAMPLED_LIFETIME,
    NUM_ENV_STEPS_SAMPLED_THIS_ITER,
    NUM_ENV_STEPS_SAMPLED_FOR_EVALUATION_THIS_ITER,
    NUM_ENV_STEPS_TRAINED,
    NUM_ENV_STEPS_TRAINED_LIFETIME,
    NUM_EPISODES,
    NUM_EPISODES_LIFETIME,
    RESTORE_WORKERS_TIMER,
    RESTORE_EVAL_WORKERS_TIMER,
    SYNCH_ENV_CONNECTOR_STATES_TIMER,
    SYNCH_EVAL_ENV_CONNECTOR_STATES_TIMER,
    SYNCH_WORKER_WEIGHTS_TIMER,
    TIMERS,
    TRAINING_ITERATION_TIMER,
    TRAINING_STEP_TIMER,
    STEPS_TRAINED_THIS_ITER_COUNTER,
)
from ray.rllib.utils.metrics.learner_info import LEARNER_INFO
from ray.rllib.utils.metrics.metrics_logger import MetricsLogger
from ray.rllib.utils.metrics.stats import Stats
from ray.rllib.utils.policy import validate_policy_id
from ray.rllib.utils.replay_buffers import MultiAgentReplayBuffer, ReplayBuffer
from ray.rllib.utils.serialization import deserialize_type, NOT_SERIALIZABLE
from ray.rllib.utils.spaces import space_utils
from ray.rllib.utils.typing import (
    AgentConnectorDataType,
    AgentID,
    AgentToModuleMappingFn,
    AlgorithmConfigDict,
    EnvCreator,
    EnvInfoDict,
    EnvType,
    EpisodeID,
    ModuleID,
    PartialAlgorithmConfigDict,
    PolicyID,
    PolicyState,
    ResultDict,
    SampleBatchType,
    ShouldModuleBeUpdatedFn,
    TensorStructType,
    TensorType,
)
from ray.train.constants import DEFAULT_STORAGE_PATH
from ray.tune.execution.placement_groups import PlacementGroupFactory
from ray.tune.experiment.trial import ExportFormat
from ray.tune.logger import Logger, UnifiedLogger
from ray.tune.registry import ENV_CREATOR, _global_registry
from ray.tune.resources import Resources
from ray.tune.trainable import Trainable
from ray.util import log_once
from ray.util.timer import _Timer
from ray.tune.registry import get_trainable_cls

if TYPE_CHECKING:
    from ray.rllib.core.learner.learner_group import LearnerGroup

try:
    from ray.rllib.extensions import AlgorithmBase
except ImportError:

    class AlgorithmBase:
        @staticmethod
        def _get_learner_bundles(cf: AlgorithmConfig) -> List[Dict[str, int]]:
            """Selects the right resource bundles for learner workers based off of cf.

            Args:
                cf: The algorithm config.

            Returns:
                A list of resource bundles for the learner workers.
            """
            if cf.num_learner_workers > 0:
                if cf.num_gpus_per_learner_worker:
                    learner_bundles = [
                        {"GPU": cf.num_learner_workers * cf.num_gpus_per_learner_worker}
                    ]
                elif cf.num_cpus_per_learner_worker:
                    learner_bundles = [
                        {
                            "CPU": cf.num_cpus_per_learner_worker
                            * cf.num_learner_workers,
                        }
                    ]
            else:
                learner_bundles = [
                    {
                        # sampling and training is not done concurrently when local is
                        # used, so pick the max.
                        "CPU": max(
                            cf.num_cpus_per_learner_worker, cf.num_cpus_for_local_worker
                        ),
                        "GPU": cf.num_gpus_per_learner_worker,
                    }
                ]
            return learner_bundles


tf1, tf, tfv = try_import_tf()

logger = logging.getLogger(__name__)


@PublicAPI
class Algorithm(Trainable, AlgorithmBase):
    """An RLlib algorithm responsible for optimizing one or more Policies.

    Algorithms contain a EnvRunnerGroup under `self.workers`. An EnvRunnerGroup is
    normally composed of a single local EnvRunner (self.workers.local_worker()), serving
    as the main copy of the NeuralNetwork(s) to be trained and optionally one or more
    remote EnvRunners used to generate environment samples in parallel.
    EnvRunnerGroup is fault tolerant and elastic. It tracks health states for all
    the managed remote EnvRunner actors. As a result, Algorithm should never
    access the underlying actor handles directly. Instead, always access them
    via all the foreach APIs with assigned IDs of the underlying EnvRunners.

    Each EnvRunners (remotes or local) contains a PolicyMap, which itself
    may contain either one policy for single-agent training or one or more
    policies for multi-agent training. Policies are synchronized
    automatically from time to time using ray.remote calls. The exact
    synchronization logic depends on the specific algorithm used,
    but this usually happens from local worker to all remote workers and
    after each training update.

    You can write your own Algorithm classes by sub-classing from `Algorithm`
    or any of its built-in sub-classes.
    This allows you to override the `training_step` method to implement
    your own algorithm logic. You can find the different built-in
    algorithms' `training_step()` methods in their respective main .py files,
    e.g. rllib.algorithms.dqn.dqn.py or rllib.algorithms.impala.impala.py.

    The most important API methods a Algorithm exposes are `train()`,
    `evaluate()`, `save()` and `restore()`.
    """

    # Whether to allow unknown top-level config keys.
    _allow_unknown_configs = False

    # List of top-level keys with value=dict, for which new sub-keys are
    # allowed to be added to the value dict.
    _allow_unknown_subkeys = [
        "tf_session_args",
        "local_tf_session_args",
        "env_config",
        "model",
        "optimizer",
        "custom_resources_per_worker",
        "evaluation_config",
        "exploration_config",
        "replay_buffer_config",
        "extra_python_environs_for_worker",
        "input_config",
        "output_config",
    ]

    # List of top level keys with value=dict, for which we always override the
    # entire value (dict), iff the "type" key in that value dict changes.
    _override_all_subkeys_if_type_changes = [
        "exploration_config",
        "replay_buffer_config",
    ]

    # List of keys that are always fully overridden if present in any dict or sub-dict
    _override_all_key_list = ["off_policy_estimation_methods", "policies"]

    _progress_metrics = (
        f"{ENV_RUNNER_RESULTS}/episode_return_mean",
        f"{EVALUATION_RESULTS}/{ENV_RUNNER_RESULTS}/episode_return_mean",
        f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}",
        f"{NUM_ENV_STEPS_TRAINED_LIFETIME}",
        f"{NUM_EPISODES_LIFETIME}",
        f"{ENV_RUNNER_RESULTS}/episode_len_mean",
    )

    @staticmethod
    def from_checkpoint(
        checkpoint: Union[str, Checkpoint],
        policy_ids: Optional[Container[PolicyID]] = None,
        policy_mapping_fn: Optional[Callable[[AgentID, EpisodeID], PolicyID]] = None,
        policies_to_train: Optional[
            Union[
                Container[PolicyID],
                Callable[[PolicyID, Optional[SampleBatchType]], bool],
            ]
        ] = None,
    ) -> "Algorithm":
        """Creates a new algorithm instance from a given checkpoint.

        Note: This method must remain backward compatible from 2.0.0 on.

        Args:
            checkpoint: The path (str) to the checkpoint directory to use
                or an AIR Checkpoint instance to restore from.
            policy_ids: Optional list of PolicyIDs to recover. This allows users to
                restore an Algorithm with only a subset of the originally present
                Policies.
            policy_mapping_fn: An optional (updated) policy mapping function
                to use from here on.
            policies_to_train: An optional list of policy IDs to be trained
                or a callable taking PolicyID and SampleBatchType and
                returning a bool (trainable or not?).
                If None, will keep the existing setup in place. Policies,
                whose IDs are not in the list (or for which the callable
                returns False) will not be updated.

        Returns:
            The instantiated Algorithm.
        """
        checkpoint_info = get_checkpoint_info(checkpoint)

        # Not possible for (v0.1) (algo class and config information missing
        # or very hard to retrieve).
        if checkpoint_info["checkpoint_version"] == version.Version("0.1"):
            raise ValueError(
                "Cannot restore a v0 checkpoint using `Algorithm.from_checkpoint()`!"
                "In this case, do the following:\n"
                "1) Create a new Algorithm object using your original config.\n"
                "2) Call the `restore()` method of this algo object passing it"
                " your checkpoint dir or AIR Checkpoint object."
            )
        elif checkpoint_info["checkpoint_version"] < version.Version("1.0"):
            raise ValueError(
                "`checkpoint_info['checkpoint_version']` in `Algorithm.from_checkpoint"
                "()` must be 1.0 or later! You are using a checkpoint with "
                f"version v{checkpoint_info['checkpoint_version']}."
            )

        # This is a msgpack checkpoint.
        if checkpoint_info["format"] == "msgpack":
            # User did not provide unserializable function with this call
            # (`policy_mapping_fn`). Note that if `policies_to_train` is None, it
            # defaults to training all policies (so it's ok to not provide this here).
            if policy_mapping_fn is None:
                # Only DEFAULT_POLICY_ID present in this algorithm, provide default
                # implementations of these two functions.
                if checkpoint_info["policy_ids"] == {DEFAULT_POLICY_ID}:
                    policy_mapping_fn = AlgorithmConfig.DEFAULT_POLICY_MAPPING_FN
                # Provide meaningful error message.
                else:
                    raise ValueError(
                        "You are trying to restore a multi-agent algorithm from a "
                        "`msgpack` formatted checkpoint, which do NOT store the "
                        "`policy_mapping_fn` or `policies_to_train` "
                        "functions! Make sure that when using the "
                        "`Algorithm.from_checkpoint()` utility, you also pass the "
                        "args: `policy_mapping_fn` and `policies_to_train` with your "
                        "call. You might leave `policies_to_train=None` in case "
                        "you would like to train all policies anyways."
                    )

        state = Algorithm._checkpoint_info_to_algorithm_state(
            checkpoint_info=checkpoint_info,
            policy_ids=policy_ids,
            policy_mapping_fn=policy_mapping_fn,
            policies_to_train=policies_to_train,
        )

        return Algorithm.from_state(state)

    @staticmethod
    def from_state(state: Dict) -> "Algorithm":
        """Recovers an Algorithm from a state object.

        The `state` of an instantiated Algorithm can be retrieved by calling its
        `get_state` method. It contains all information necessary
        to create the Algorithm from scratch. No access to the original code (e.g.
        configs, knowledge of the Algorithm's class, etc..) is needed.

        Args:
            state: The state to recover a new Algorithm instance from.

        Returns:
            A new Algorithm instance.
        """
        algorithm_class: Type[Algorithm] = state.get("algorithm_class")
        if algorithm_class is None:
            raise ValueError(
                "No `algorithm_class` key was found in given `state`! "
                "Cannot create new Algorithm."
            )
        # algo_class = get_trainable_cls(algo_class_name)
        # Create the new algo.
        config = state.get("config")
        if not config:
            raise ValueError("No `config` found in given Algorithm state!")
        new_algo = algorithm_class(config=config)
        # Set the new algo's state.
        new_algo.__setstate__(state)

        # Return the new algo.
        return new_algo

    @PublicAPI
    def __init__(
        self,
        config: Optional[AlgorithmConfig] = None,
        env=None,  # deprecated arg
        logger_creator: Optional[Callable[[], Logger]] = None,
        **kwargs,
    ):
        """Initializes an Algorithm instance.

        Args:
            config: Algorithm-specific configuration object.
            logger_creator: Callable that creates a ray.tune.Logger
                object. If unspecified, a default logger is created.
            **kwargs: Arguments passed to the Trainable base class.
        """
        config = config or self.get_default_config()

        # Translate possible dict into an AlgorithmConfig object, as well as,
        # resolving generic config objects into specific ones (e.g. passing
        # an `AlgorithmConfig` super-class instance into a PPO constructor,
        # which normally would expect a PPOConfig object).
        if isinstance(config, dict):
            default_config = self.get_default_config()
            # `self.get_default_config()` also returned a dict ->
            # Last resort: Create core AlgorithmConfig from merged dicts.
            if isinstance(default_config, dict):
                config = AlgorithmConfig.from_dict(
                    config_dict=self.merge_algorithm_configs(
                        default_config, config, True
                    )
                )
            # Default config is an AlgorithmConfig -> update its properties
            # from the given config dict.
            else:
                config = default_config.update_from_dict(config)
        else:
            default_config = self.get_default_config()
            # Given AlgorithmConfig is not of the same type as the default config:
            # This could be the case e.g. if the user is building an algo from a
            # generic AlgorithmConfig() object.
            if not isinstance(config, type(default_config)):
                config = default_config.update_from_dict(config.to_dict())

        # In case this algo is using a generic config (with no algo_class set), set it
        # here.
        if config.algo_class is None:
            config.algo_class = type(self)

        if env is not None:
            deprecation_warning(
                old=f"algo = Algorithm(env='{env}', ...)",
                new=f"algo = AlgorithmConfig().environment('{env}').build()",
                error=False,
            )
            config.environment(env)

        # Validate and freeze our AlgorithmConfig object (no more changes possible).
        config.validate()
        config.freeze()

        # Convert `env` provided in config into a concrete env creator callable, which
        # takes an EnvContext (config dict) as arg and returning an RLlib supported Env
        # type (e.g. a gym.Env).
        self._env_id, self.env_creator = self._get_env_id_and_creator(
            config.env, config
        )
        env_descr = (
            self._env_id.__name__ if isinstance(self._env_id, type) else self._env_id
        )

        # Placeholder for a local replay buffer instance.
        self.local_replay_buffer = None

        # Placeholder for our LearnerGroup responsible for updating the RLModule(s).
        self.learner_group: Optional["LearnerGroup"] = None

        # The Algorithm's `MetricsLogger` object to collect stats from all its
        # components (including timers, counters and other stats in its own
        # `training_step()` and other methods) as well as custom callbacks.
        self.metrics = MetricsLogger()
        # Initialize lifetime counters.
        self.metrics.log_dict(
            {
                NUM_ENV_STEPS_SAMPLED_LIFETIME: 0,
                NUM_AGENT_STEPS_SAMPLED_LIFETIME: {DEFAULT_AGENT_ID: 0},
                NUM_ENV_STEPS_TRAINED_LIFETIME: 0,
                NUM_AGENT_STEPS_TRAINED_LIFETIME: {DEFAULT_AGENT_ID: 0},
            },
            reduce="sum",
        )

        # Create a default logger creator if no logger_creator is specified
        if logger_creator is None:
            # Default logdir prefix containing the agent's name and the
            # env id.
            timestr = datetime.today().strftime("%Y-%m-%d_%H-%M-%S")
            env_descr_for_dir = re.sub("[/\\\\]", "-", str(env_descr))
            logdir_prefix = f"{str(self)}_{env_descr_for_dir}_{timestr}"
            if not os.path.exists(DEFAULT_STORAGE_PATH):
                # Possible race condition if dir is created several times on
                # rollout workers
                os.makedirs(DEFAULT_STORAGE_PATH, exist_ok=True)
            logdir = tempfile.mkdtemp(prefix=logdir_prefix, dir=DEFAULT_STORAGE_PATH)

            # Allow users to more precisely configure the created logger
            # via "logger_config.type".
            if config.logger_config and "type" in config.logger_config:

                def default_logger_creator(config):
                    """Creates a custom logger with the default prefix."""
                    cfg = config["logger_config"].copy()
                    cls = cfg.pop("type")
                    # Provide default for logdir, in case the user does
                    # not specify this in the "logger_config" dict.
                    logdir_ = cfg.pop("logdir", logdir)
                    return from_config(cls=cls, _args=[cfg], logdir=logdir_)

            # If no `type` given, use tune's UnifiedLogger as last resort.
            else:

                def default_logger_creator(config):
                    """Creates a Unified logger with the default prefix."""
                    return UnifiedLogger(config, logdir, loggers=None)

            logger_creator = default_logger_creator

        # Metrics-related properties.
        self._timers = defaultdict(_Timer)
        self._counters = defaultdict(int)
        self._episode_history = []
        self._episodes_to_be_collected = []

        # The fully qualified AlgorithmConfig used for evaluation
        # (or None if evaluation not setup).
        self.evaluation_config: Optional[AlgorithmConfig] = None
        # Evaluation EnvRunnerGroup and metrics last returned by `self.evaluate()`.
        self.evaluation_workers: Optional[EnvRunnerGroup] = None
        # Initialize common evaluation_metrics to nan, before they become
        # available. We want to make sure the metrics are always present
        # (although their values may be nan), so that Tune does not complain
        # when we use these as stopping criteria.
        self.evaluation_metrics = {
            # TODO: Don't dump sampler results into top-level.
            "evaluation": {
                "episode_reward_max": np.nan,
                "episode_reward_min": np.nan,
                "episode_reward_mean": np.nan,
                "sampler_results": {
                    "episode_reward_max": np.nan,
                    "episode_reward_min": np.nan,
                    "episode_reward_mean": np.nan,
                },
            },
        }

        super().__init__(
            config=config,
            logger_creator=logger_creator,
            **kwargs,
        )

    @OverrideToImplementCustomLogic
    @classmethod
    def get_default_config(cls) -> AlgorithmConfig:
        return AlgorithmConfig()

    @OverrideToImplementCustomLogic
    def _remote_worker_ids_for_metrics(self) -> List[int]:
        """Returns a list of remote worker IDs to fetch metrics from.

        Specific Algorithm implementations can override this method to
        use a subset of the workers for metrics collection.

        Returns:
            List of remote worker IDs to fetch metrics from.
        """
        return self.workers.healthy_worker_ids()

    @OverrideToImplementCustomLogic_CallToSuperRecommended
    @override(Trainable)
    def setup(self, config: AlgorithmConfig) -> None:
        # Setup our config: Merge the user-supplied config dict (which could
        # be a partial config dict) with the class' default.
        if not isinstance(config, AlgorithmConfig):
            assert isinstance(config, PartialAlgorithmConfigDict)
            config_obj = self.get_default_config()
            if not isinstance(config_obj, AlgorithmConfig):
                assert isinstance(config, PartialAlgorithmConfigDict)
                config_obj = AlgorithmConfig().from_dict(config_obj)
            config_obj.update_from_dict(config)
            config_obj.env = self._env_id
            self.config = config_obj

        # Set Algorithm's seed after we have - if necessary - enabled
        # tf eager-execution.
        update_global_seed_if_necessary(self.config.framework_str, self.config.seed)

        self._record_usage(self.config)

        # Create the callbacks object.
        self.callbacks = self.config.callbacks_class()

        if self.config.log_level in ["WARN", "ERROR"]:
            logger.info(
                f"Current log_level is {self.config.log_level}. For more information, "
                "set 'log_level': 'INFO' / 'DEBUG' or use the -v and "
                "-vv flags."
            )
        if self.config.log_level:
            logging.getLogger("ray.rllib").setLevel(self.config.log_level)

        # Create local replay buffer if necessary.
        self.local_replay_buffer = self._create_local_replay_buffer_if_necessary(
            self.config
        )

        # Create a dict, mapping ActorHandles to sets of open remote
        # requests (object refs). This way, we keep track, of which actors
        # inside this Algorithm (e.g. a remote EnvRunner) have
        # already been sent how many (e.g. `sample()`) requests.
        self.remote_requests_in_flight: DefaultDict[
            ActorHandle, Set[ray.ObjectRef]
        ] = defaultdict(set)

        self.workers: Optional[EnvRunnerGroup] = None

        # Offline RL settings.
        input_evaluation = self.config.get("input_evaluation")
        if input_evaluation is not None and input_evaluation is not DEPRECATED_VALUE:
            ope_dict = {str(ope): {"type": ope} for ope in input_evaluation}
            deprecation_warning(
                old="config.input_evaluation={}".format(input_evaluation),
                new="config.evaluation(evaluation_config=config.overrides("
                f"off_policy_estimation_methods={ope_dict}"
                "))",
                error=True,
                help="Running OPE during training is not recommended.",
            )
            self.config.off_policy_estimation_methods = ope_dict

        # Create a set of env runner actors via a EnvRunnerGroup.
        self.workers = EnvRunnerGroup(
            env_creator=self.env_creator,
            validate_env=self.validate_env,
            default_policy_class=self.get_default_policy_class(self.config),
            config=self.config,
            num_workers=self.config.num_env_runners,
            local_worker=True,
            logdir=self.logdir,
        )

        # Ensure remote workers are initially in sync with the local worker.
        self.workers.sync_weights(inference_only=True)

        # Compile, validate, and freeze an evaluation config.
        self.evaluation_config = self.config.get_evaluation_config_object()
        self.evaluation_config.validate()
        self.evaluation_config.freeze()

        # Evaluation EnvRunnerGroup setup.
        # User would like to setup a separate evaluation worker set.
        # Note: We skip EnvRunnerGroup creation if we need to do offline evaluation.
        if self._should_create_evaluation_rollout_workers(self.evaluation_config):
            _, env_creator = self._get_env_id_and_creator(
                self.evaluation_config.env, self.evaluation_config
            )

            # Create a separate evaluation worker set for evaluation.
            # If evaluation_num_env_runners=0, use the evaluation set's local
            # worker for evaluation, otherwise, use its remote workers
            # (parallelized evaluation).
            self.evaluation_workers: EnvRunnerGroup = EnvRunnerGroup(
                env_creator=env_creator,
                validate_env=None,
                default_policy_class=self.get_default_policy_class(self.config),
                config=self.evaluation_config,
                num_workers=self.config.evaluation_num_env_runners,
                logdir=self.logdir,
            )

        self.evaluation_dataset = None
        if (
            self.evaluation_config.off_policy_estimation_methods
            and not self.evaluation_config.ope_split_batch_by_episode
        ):
            # the num worker is set to 0 to avoid creating shards. The dataset will not
            # be repartioned to num_workers blocks.
            logger.info("Creating evaluation dataset ...")
            self.evaluation_dataset, _ = get_dataset_and_shards(
                self.evaluation_config, num_workers=0
            )
            logger.info("Evaluation dataset created")

        self.reward_estimators: Dict[str, OffPolicyEstimator] = {}
        ope_types = {
            "is": ImportanceSampling,
            "wis": WeightedImportanceSampling,
            "dm": DirectMethod,
            "dr": DoublyRobust,
        }
        for name, method_config in self.config.off_policy_estimation_methods.items():
            method_type = method_config.pop("type")
            if method_type in ope_types:
                deprecation_warning(
                    old=method_type,
                    new=str(ope_types[method_type]),
                    error=True,
                )
                method_type = ope_types[method_type]
            elif isinstance(method_type, str):
                logger.log(0, "Trying to import from string: " + method_type)
                mod, obj = method_type.rsplit(".", 1)
                mod = importlib.import_module(mod)
                method_type = getattr(mod, obj)
            if isinstance(method_type, type) and issubclass(
                method_type, OfflineEvaluator
            ):
                # TODO(kourosh) : Add an integration test for all these
                # offline evaluators.
                policy = self.get_policy()
                if issubclass(method_type, OffPolicyEstimator):
                    method_config["gamma"] = self.config.gamma
                self.reward_estimators[name] = method_type(policy, **method_config)
            else:
                raise ValueError(
                    f"Unknown off_policy_estimation type: {method_type}! Must be "
                    "either a class path or a sub-class of ray.rllib."
                    "offline.offline_evaluator::OfflineEvaluator"
                )
            # TODO (Rohan138): Refactor this and remove deprecated methods
            # Need to add back method_type in case Algorithm is restored from checkpoint
            method_config["type"] = method_type

        self.learner_group = None
        if self.config.enable_rl_module_and_learner:
            local_worker = self.workers.local_worker()
            env = spaces = None
            # EnvRunners have a `module` property, which stores the RLModule
            # (or MARLModule, which is a subclass of RLModule, in the multi-agent case).
            if hasattr(local_worker, "module") and local_worker.module is not None:
                marl_module_dict = dict(local_worker.module.as_multi_agent())
                env = local_worker.env
                spaces = {
                    mid: (mod.config.observation_space, mod.config.action_space)
                    for mid, mod in marl_module_dict.items()
                }
                policy_dict, _ = self.config.get_multi_agent_setup(
                    env=env, spaces=spaces
                )
                module_spec: MultiAgentRLModuleSpec = self.config.get_marl_module_spec(
                    policy_dict=policy_dict
                )
            # TODO (Sven): Deprecate this path: Old stack API RolloutWorkers and
            #  DreamerV3's EnvRunners have a `marl_module_spec` property.
            elif hasattr(local_worker, "marl_module_spec"):
                module_spec: MultiAgentRLModuleSpec = local_worker.marl_module_spec
            else:
                raise AttributeError(
                    "Your local EnvRunner/RolloutWorker does NOT have any property "
                    "referring to its RLModule!"
                )
            self.learner_group = self.config.build_learner_group(
                rl_module_spec=module_spec, env=env, spaces=spaces
            )

            # Check if there are modules to load from the `module_spec`.
            rl_module_ckpt_dirs = {}
            marl_module_ckpt_dir = module_spec.load_state_path
            modules_to_load = module_spec.modules_to_load
            for module_id, sub_module_spec in module_spec.module_specs.items():
                if sub_module_spec.load_state_path:
                    rl_module_ckpt_dirs[module_id] = sub_module_spec.load_state_path
            if marl_module_ckpt_dir or rl_module_ckpt_dirs:
                self.learner_group.load_module_state(
                    marl_module_ckpt_dir=marl_module_ckpt_dir,
                    modules_to_load=modules_to_load,
                    rl_module_ckpt_dirs=rl_module_ckpt_dirs,
                )

            # Only when using RolloutWorkers: Update also the worker set's
            # `is_policy_to_train`.
            # Note that with the new EnvRunner API in combination with the new stack,
            # this information only needs to be kept in the Learner and not on the
            # EnvRunners anymore.
            if not self.config.enable_env_runner_and_connector_v2:
                policies_to_train = self.config.policies_to_train or set(
                    self.config.policies
                )
                self.workers.foreach_worker(
                    lambda w: w.set_is_policy_to_train(policies_to_train),
                )

            # Sync the weights from the learner group to the rollout workers.
            weights = self.learner_group.get_weights(
                inference_only=self.config.enable_env_runner_and_connector_v2
            )
            local_worker.set_weights(weights)
            self.workers.sync_weights(inference_only=True)

        # Run `on_algorithm_init` callback after initialization is done.
        self.callbacks.on_algorithm_init(algorithm=self, metrics_logger=self.metrics)

    @OverrideToImplementCustomLogic
    @classmethod
    def get_default_policy_class(
        cls,
        config: AlgorithmConfig,
    ) -> Optional[Type[Policy]]:
        """Returns a default Policy class to use, given a config.

        This class will be used by an Algorithm in case
        the policy class is not provided by the user in any single- or
        multi-agent PolicySpec.

        Note: This method is ignored when the RLModule API is enabled.
        """
        return None

    @override(Trainable)
    def step(self) -> ResultDict:
        """Implements the main `Algorithm.train()` logic.

        Takes n attempts to perform a single training step. Thereby
        catches RayErrors resulting from worker failures. After n attempts,
        fails gracefully.

        Override this method in your Algorithm sub-classes if you would like to
        handle worker failures yourself.
        Otherwise, override only `training_step()` to implement the core
        algorithm logic.

        Returns:
            The results dict with stats/infos on sampling, training,
            and - if required - evaluation.
        """
        # Do we have to run `self.evaluate()` this iteration?
        # `self.iteration` gets incremented after this function returns,
        # meaning that e.g. the first time this function is called,
        # self.iteration will be 0.
        evaluate_this_iter = (
            self.config.evaluation_interval
            and (self.iteration + 1) % self.config.evaluation_interval == 0
        )
        evaluate_in_general = bool(self.config.evaluation_interval)

        # Results dict for training (and if appolicable: evaluation).
        train_results: ResultDict = {}
        eval_results: ResultDict = {}

        # Parallel eval + training (BUT w/o using a thread pool):
        if (
            evaluate_this_iter
            and self.config.evaluation_parallel_to_training
            and self.config._evaluation_parallel_to_training_wo_thread
        ):
            (
                train_results,
                eval_results,
                train_iter_ctx,
            ) = self._run_one_training_iteration_and_evaluation_in_parallel_wo_thread()
        # Parallel eval + training: Kick off evaluation-loop and parallel train() call.
        elif self.config._run_training_always_in_thread or (
            evaluate_this_iter and self.config.evaluation_parallel_to_training
        ):
            (
                train_results,
                eval_results,
                train_iter_ctx,
            ) = self._run_one_training_iteration_and_evaluation_in_parallel()

        # - No evaluation necessary, just run the next training iteration.
        # - We have to evaluate in this training iteration, but no parallelism ->
        #   evaluate after the training iteration is entirely done.
        else:
            train_results, train_iter_ctx = self._run_one_training_iteration()

        # Sequential: Train (already done above), then evaluate.
        if evaluate_this_iter and not self.config.evaluation_parallel_to_training:
            eval_results = self._run_one_evaluation(parallel_train_future=None)

        # Attach latest available evaluation results to train results, if necessary.
        if (
            evaluate_in_general
            and not evaluate_this_iter
            and self.config.always_attach_evaluation_results
        ):
            if not isinstance(self.evaluation_metrics, dict):
                raise ValueError(
                    "Algorithm.evaluate() needs to return a ResultDict, but returned "
                    f"{self.evaluation_metrics}!"
                )
            eval_results = self.evaluation_metrics

        # Sync filters on workers.
        # TODO (sven): For the new API stack, the common execution pattern for any algo
        #  should be: [sample + get_metrics + get_state] -> send all these in one remote
        #  call down to `training_step` (where episodes are sent as ray object
        #  references). Then distribute the episode refs to the learners, store metrics
        #  in special key in result dict and perform the connector merge/broadcast
        #  inside the `training_step` as well. See the new IMPALA for an example.
        if self.config.enable_env_runner_and_connector_v2:
            # Synchronize EnvToModule and ModuleToEnv connector states and broadcast new
            # states back to all EnvRunners.
            with self.metrics.log_time((TIMERS, SYNCH_ENV_CONNECTOR_STATES_TIMER)):
                self.workers.sync_env_runner_states(
                    config=self.config,
                    env_steps_sampled=self.metrics.peek(NUM_ENV_STEPS_SAMPLED_LIFETIME),
                )
            # Compile final ResultDict from `train_results` and `eval_results`. Note
            # that, as opposed to the old API stack, EnvRunner stats should already be
            # in `train_results` and `eval_results`.
            results = self._compile_iteration_results_new_api_stack(
                train_results=train_results,
                eval_results=eval_results,
                step_ctx=train_iter_ctx,
            )
        else:
            self._sync_filters_if_needed(
                central_worker=self.workers.local_worker(),
                workers=self.workers,
                config=self.config,
            )
            # Get EnvRunner metrics and compile them into results.
            episodes_this_iter = collect_episodes(
                self.workers,
                self._remote_worker_ids_for_metrics(),
                timeout_seconds=self.config.metrics_episode_collection_timeout_s,
            )
            results = self._compile_iteration_results_old_and_hybrid_api_stacks(
                episodes_this_iter=episodes_this_iter,
                step_ctx=train_iter_ctx,
                iteration_results={**train_results, **eval_results},
            )

        # TODO (sven): Deprecate this API, this should be done via a custom Callback.
        #  Provide example script/update existing one.
        if self.config.env_task_fn is not None:
            if not callable(self.config.env_task_fn):
                raise ValueError(
                    "`env_task_fn` must be None or a callable taking "
                    "[train_results, env, env_ctx] as args!"
                )

            def fn(env, env_context, task_fn):
                new_task = task_fn(results, env, env_context)
                cur_task = env.get_task()
                if cur_task != new_task:
                    env.set_task(new_task)

            fn = functools.partial(fn, task_fn=self.config.env_task_fn)
            self.workers.foreach_env_with_context(fn)

        return results

    @PublicAPI
    def evaluate(
        self,
        parallel_train_future: Optional[concurrent.futures.ThreadPoolExecutor] = None,
    ) -> ResultDict:
        """Evaluates current policy under `evaluation_config` settings.

        Args:
            parallel_train_future: In case, we are training and avaluating in parallel,
                this arg carries the currently running ThreadPoolExecutor object that
                runs the training iteration. Use `parallel_train_future.done()` to
                check, whether the parallel training job has completed and
                `parallel_train_future.result()` to get its return values.

        Returns:
            A ResultDict only containing the evaluation results from the current
            iteration.
        """
        # Call the `_before_evaluate` hook.
        self._before_evaluate()

        if self.evaluation_dataset is not None:
            return self._run_offline_evaluation()

        # Sync weights to the evaluation EnvRunners.
        if self.evaluation_workers is not None:
            self.evaluation_workers.sync_weights(
                from_worker_or_learner_group=self.workers.local_worker(),
                inference_only=True,
            )

            if self.config.enable_env_runner_and_connector_v2:
                # Synchronize EnvToModule and ModuleToEnv connector states and broadcast
                # new states back to all eval EnvRunners.
                with self._timers[SYNCH_EVAL_ENV_CONNECTOR_STATES_TIMER]:
                    self.evaluation_workers.sync_env_runner_states(
                        config=self.evaluation_config,
                        from_worker=self.workers.local_worker(),
                        env_steps_sampled=self.metrics.peek(
                            NUM_ENV_STEPS_SAMPLED_LIFETIME, default=0
                        ),
                    )
            else:
                self._sync_filters_if_needed(
                    central_worker=self.workers.local_worker(),
                    workers=self.evaluation_workers,
                    config=self.evaluation_config,
                )

        self.callbacks.on_evaluate_start(algorithm=self, metrics_logger=self.metrics)

        env_steps = agent_steps = 0
        batches = []

        # We will use a user provided evaluation function.
        if self.config.custom_evaluation_function:
            eval_results = self._evaluate_with_custom_eval_function()
        # There is no eval EnvRunnerGroup -> Run on local EnvRunner.
        elif self.evaluation_workers is None:
            (
                eval_results,
                env_steps,
                agent_steps,
                batches,
            ) = self._evaluate_on_local_env_runner(self.workers.local_worker())
        # There is only a local eval EnvRunner -> Run on that.
        elif self.evaluation_workers.num_healthy_remote_workers() == 0:
            (
                eval_results,
                env_steps,
                agent_steps,
                batches,
            ) = self._evaluate_on_local_env_runner(
                self.evaluation_workers.local_worker()
            )
        # There are healthy remote evaluation workers -> Run on these.
        elif self.evaluation_workers.num_healthy_remote_workers() > 0:
            # Running in automatic duration mode (parallel with training step).
            if self.config.evaluation_duration == "auto":
                assert parallel_train_future is not None
                (
                    eval_results,
                    env_steps,
                    agent_steps,
                    batches,
                ) = self._evaluate_with_auto_duration(parallel_train_future)
            # Running with a fixed amount of data to sample.
            else:
                (
                    eval_results,
                    env_steps,
                    agent_steps,
                    batches,
                ) = self._evaluate_with_fixed_duration()
        # Can't find a good way to run this evaluation -> Wait for next iteration.
        else:
            pass

        if self.config.enable_env_runner_and_connector_v2:
            # Lifetime eval counters.
            self.metrics.log_dict(
                {
                    NUM_ENV_STEPS_SAMPLED_LIFETIME: env_steps,
                    NUM_AGENT_STEPS_SAMPLED_LIFETIME: agent_steps,
                    NUM_EPISODES_LIFETIME: self.metrics.peek(
                        (EVALUATION_RESULTS, ENV_RUNNER_RESULTS, NUM_EPISODES),
                        default=0,
                    ),
                },
                key=EVALUATION_RESULTS,
                reduce="sum",
            )
            eval_results = self.metrics.reduce(
                key=EVALUATION_RESULTS, return_stats_obj=False
            )
        else:
            eval_results = dict(
                {"sampler_results": eval_results, ENV_RUNNER_RESULTS: eval_results},
                **eval_results,
            )
            eval_results[NUM_AGENT_STEPS_SAMPLED_THIS_ITER] = agent_steps
            eval_results[NUM_ENV_STEPS_SAMPLED_THIS_ITER] = env_steps
            eval_results["timesteps_this_iter"] = env_steps
            self._counters[NUM_ENV_STEPS_SAMPLED_FOR_EVALUATION_THIS_ITER] = env_steps

        # Compute off-policy estimates
        if not self.config.custom_evaluation_function:
            estimates = defaultdict(list)
            # for each batch run the estimator's fwd pass
            for name, estimator in self.reward_estimators.items():
                for batch in batches:
                    estimate_result = estimator.estimate(
                        batch,
                        split_batch_by_episode=self.config.ope_split_batch_by_episode,
                    )
                    estimates[name].append(estimate_result)

            # collate estimates from all batches
            if estimates:
                eval_results["off_policy_estimator"] = {}
                for name, estimate_list in estimates.items():
                    avg_estimate = tree.map_structure(
                        lambda *x: np.mean(x, axis=0), *estimate_list
                    )
                    eval_results["off_policy_estimator"][name] = avg_estimate

        # Trigger `on_evaluate_end` callback.
        self.callbacks.on_evaluate_end(
            algorithm=self,
            metrics_logger=self.metrics,
            evaluation_metrics=eval_results,
        )

        # Also return the results here for convenience.
        return eval_results

    def _evaluate_with_custom_eval_function(self):
        logger.info(
            f"Evaluating current state of {self} using the custom eval function "
            f"{self.config.custom_evaluation_function}"
        )
        eval_results = self.config.custom_evaluation_function(
            self, self.evaluation_workers
        )
        if not eval_results or not isinstance(eval_results, dict):
            raise ValueError(
                "Custom eval function must return "
                f"dict of metrics! Got {eval_results}."
            )
        return eval_results

    def _evaluate_on_local_env_runner(self, env_runner):
        if hasattr(env_runner, "input_reader") and env_runner.input_reader is None:
            raise ValueError(
                "Cannot evaluate on a local worker (wether there is no evaluation "
                "EnvRunnerGroup OR no remote evaluation workers) in the Algorithm or "
                "w/o an environment on that local worker!\nTry one of the following:"
                "\n1) Set `evaluation_interval` > 0 to force creating a separate "
                "evaluation EnvRunnerGroup.\n2) Set `create_env_on_driver=True` to "
                "force the local (non-eval) EnvRunner to have an environment to "
                "evaluate on."
            )
        elif self.config.evaluation_parallel_to_training:
            raise ValueError(
                "Cannot run on local evaluation worker parallel to training! Try "
                "setting `evaluation_parallel_to_training=False`."
            )

        # How many episodes/timesteps do we need to run?
        unit = self.config.evaluation_duration_unit
        duration = self.config.evaluation_duration
        eval_cfg = self.evaluation_config

        env_steps = agent_steps = 0

        logger.info(f"Evaluating current state of {self} for {duration} {unit}.")

        all_batches = []
        if self.config.enable_env_runner_and_connector_v2:
            episodes = env_runner.sample(
                num_timesteps=duration if unit == "timesteps" else None,
                num_episodes=duration if unit == "episodes" else None,
            )
            agent_steps += sum(e.agent_steps() for e in episodes)
            env_steps += sum(e.env_steps() for e in episodes)
        elif unit == "episodes":
            for _ in range(duration):
                batch = env_runner.sample()
                agent_steps += batch.agent_steps()
                env_steps += batch.env_steps()
                if self.reward_estimators:
                    all_batches.append(batch)
        else:
            batch = env_runner.sample()
            agent_steps += batch.agent_steps()
            env_steps += batch.env_steps()
            if self.reward_estimators:
                all_batches.append(batch)

        env_runner_results = env_runner.get_metrics()

        if not self.config.enable_env_runner_and_connector_v2:
            env_runner_results = summarize_episodes(
                env_runner_results,
                env_runner_results,
                keep_custom_metrics=eval_cfg.keep_per_episode_custom_metrics,
            )
        else:
            self.metrics.log_dict(
                env_runner_results,
                key=(EVALUATION_RESULTS, ENV_RUNNER_RESULTS),
            )
            env_runner_results = None

        return env_runner_results, env_steps, agent_steps, all_batches

    def _evaluate_with_auto_duration(self, parallel_train_future):
        logger.info(
            f"Evaluating current state of {self} for as long as the parallelly "
            "running training step takes."
        )

        all_metrics = []
        all_batches = []

        # How many episodes have we run (across all eval workers)?
        num_healthy_workers = self.evaluation_workers.num_healthy_remote_workers()
        # Do we have to force-reset the EnvRunners before the first round of `sample()`
        # calls.?
        force_reset = self.config.evaluation_force_reset_envs_before_iteration

        # Remote function used on healthy EnvRunners to sample, get metrics, and
        # step counts.
        def _env_runner_remote(worker, num, round, iter):
            # Sample AND get_metrics, but only return metrics (and steps actually taken)
            # to save time.
            episodes = worker.sample(
                num_timesteps=num, force_reset=force_reset and round == 0
            )
            metrics = worker.get_metrics()
            env_steps = sum(e.env_steps() for e in episodes)
            agent_steps = sum(e.agent_steps() for e in episodes)
            return env_steps, agent_steps, metrics, iter

        env_steps = agent_steps = 0
        train_mean_time = self._timers[TRAINING_ITERATION_TIMER].mean
        t0 = time.time()
        algo_iteration = self.iteration

        _round = -1
        while (
            # In case all the remote evaluation workers die during a round of
            # evaluation, we need to stop.
            num_healthy_workers > 0
            # Run at least for one round AND at least for as long as the parallel
            # training step takes.
            and (_round == -1 or not parallel_train_future.done())
        ):
            _round += 1
            # New API stack -> EnvRunners return Episodes.
            if self.config.enable_env_runner_and_connector_v2:
                # Compute rough number of timesteps it takes for a single EnvRunner
                # to occupy the estimated (parallelly running) train step.
                _num = min(
                    # Cap at 20k to not put too much memory strain on EnvRunners.
                    20000,
                    max(
                        # Low-cap at 100 to avoid possibly negative rollouts or very
                        # short ones.
                        100,
                        (
                            # How much time do we have left?
                            (train_mean_time - (time.time() - t0))
                            # Multiply by our own (eval) throughput to get the timesteps
                            # to do (per worker).
                            * self._timers[EVALUATION_ITERATION_TIMER].mean_throughput
                            / num_healthy_workers
                        ),
                    ),
                )

                self.evaluation_workers.foreach_worker_async(
                    func=functools.partial(
                        _env_runner_remote, num=_num, round=_round, iter=algo_iteration
                    ),
                )
                results = self.evaluation_workers.fetch_ready_async_reqs(
                    return_obj_refs=False, timeout_seconds=0.01
                )
                for wid, (env_s, ag_s, metrics, iter) in results:
                    if iter != self.iteration:
                        continue
                    env_steps += env_s
                    agent_steps += ag_s
                    all_metrics.append(metrics)
            # Old API stack -> RolloutWorkers return batches.
            else:
                self.evaluation_workers.foreach_worker_async(
                    func=lambda w: (w.sample(), w.get_metrics(), algo_iteration),
                )
                results = self.evaluation_workers.fetch_ready_async_reqs(
                    return_obj_refs=False, timeout_seconds=0.01
                )
                for wid, (batch, metrics, iter) in results:
                    if iter != self.iteration:
                        continue
                    env_steps += batch.env_steps()
                    agent_steps += batch.agent_steps()
                    all_metrics.extend(metrics)
                    if self.reward_estimators:
                        # TODO: (kourosh) This approach will cause an OOM issue when
                        #  the dataset gets huge (should be ok for now).
                        all_batches.append(batch)

            # Update correct number of healthy remote workers.
            num_healthy_workers = self.evaluation_workers.num_healthy_remote_workers()

        if num_healthy_workers == 0:
            logger.warning(
                "Calling `sample()` on your remote evaluation worker(s) "
                "resulted in all workers crashing! Make sure a) your environment is not"
                " too unstable, b) you have enough evaluation workers "
                "(`config.evaluation(evaluation_num_env_runners=...)`) to cover for "
                "occasional losses, and c) you use the `config.fault_tolerance("
                "recreate_failed_env_runners=True)` setting."
            )

        if not self.config.enable_env_runner_and_connector_v2:
            env_runner_results = summarize_episodes(
                all_metrics,
                all_metrics,
                keep_custom_metrics=(
                    self.evaluation_config.keep_per_episode_custom_metrics
                ),
            )
            num_episodes = env_runner_results[NUM_EPISODES]
        else:
            self.metrics.log_n_dicts(
                all_metrics,
                key=(EVALUATION_RESULTS, ENV_RUNNER_RESULTS),
            )
            num_episodes = self.metrics.peek(
                (EVALUATION_RESULTS, ENV_RUNNER_RESULTS, NUM_EPISODES),
                default=0,
            )
            env_runner_results = None

        # Warn if results are empty, it could be that this is because the auto-time is
        # not enough to run through one full episode.
        if (
            self.config.evaluation_force_reset_envs_before_iteration
            and num_episodes == 0
        ):
            logger.warning(
                "This evaluation iteration resulted in an empty set of episode summary "
                "results! It's possible that the auto-duration time (roughly the mean "
                "time it takes for the training step to finish) is not enough to finish"
                " even a single episode. Your current mean training iteration time is "
                f"{train_mean_time}sec. Try setting the min iteration time to a higher "
                "value via the `config.reporting(min_time_s_per_iteration=...)` OR you "
                "can also set `config.evaluation_force_reset_envs_before_iteration` to "
                "False. However, keep in mind that then the evaluation results may "
                "contain some episode stats generated with earlier weights versions."
            )

        return env_runner_results, env_steps, agent_steps, all_batches

    def _evaluate_with_fixed_duration(self):
        # How many episodes/timesteps do we need to run?
        unit = self.config.evaluation_duration_unit
        eval_cfg = self.evaluation_config
        num_workers = self.config.evaluation_num_env_runners
        force_reset = self.config.evaluation_force_reset_envs_before_iteration
        time_out = self.config.evaluation_sample_timeout_s

        # Remote function used on healthy EnvRunners to sample, get metrics, and
        # step counts.
        def _env_runner_remote(worker, num, round, iter):
            # Sample AND get_metrics, but only return metrics (and steps actually taken)
            # to save time. Also return the iteration to check, whether we should
            # discard and outdated result (from a slow worker).
            episodes = worker.sample(
                num_timesteps=(
                    num[worker.worker_index] if unit == "timesteps" else None
                ),
                num_episodes=(num[worker.worker_index] if unit == "episodes" else None),
                force_reset=force_reset and round == 0,
            )
            metrics = worker.get_metrics()
            env_steps = sum(e.env_steps() for e in episodes)
            agent_steps = sum(e.agent_steps() for e in episodes)
            return env_steps, agent_steps, metrics, iter

        all_metrics = []
        all_batches = []

        # How many episodes have we run (across all eval workers)?
        num_units_done = 0
        num_healthy_workers = self.evaluation_workers.num_healthy_remote_workers()

        env_steps = agent_steps = 0

        t_last_result = time.time()
        _round = -1
        algo_iteration = self.iteration

        # In case all the remote evaluation workers die during a round of
        # evaluation, we need to stop.
        while num_healthy_workers > 0:
            units_left_to_do = self.config.evaluation_duration - num_units_done
            if units_left_to_do <= 0:
                break

            _round += 1

            # New API stack -> EnvRunners return Episodes.
            if self.config.enable_env_runner_and_connector_v2:
                _num = [None] + [  # [None]: skip idx=0 (local worker)
                    (units_left_to_do // num_healthy_workers)
                    + bool(i <= (units_left_to_do % num_healthy_workers))
                    for i in range(1, num_workers + 1)
                ]
                self.evaluation_workers.foreach_worker_async(
                    func=functools.partial(
                        _env_runner_remote, num=_num, round=_round, iter=algo_iteration
                    ),
                )
                results = self.evaluation_workers.fetch_ready_async_reqs(
                    return_obj_refs=False, timeout_seconds=0.01
                )
                # Make sure we properly time out if we have not received any results
                # for more than `time_out` seconds.
                time_now = time.time()
                if not results and time_now - t_last_result > time_out:
                    break
                elif results:
                    t_last_result = time_now
                for wid, (env_s, ag_s, met, iter) in results:
                    if iter != self.iteration:
                        continue
                    env_steps += env_s
                    agent_steps += ag_s
                    all_metrics.append(met)
                    num_units_done += (
                        met[NUM_EPISODES].peek()
                        if unit == "episodes"
                        else (
                            env_s if self.config.count_steps_by == "env_steps" else ag_s
                        )
                    )
            # Old API stack -> RolloutWorkers return batches.
            else:
                units_per_healthy_remote_worker = (
                    1
                    if unit == "episodes"
                    else eval_cfg.rollout_fragment_length
                    * eval_cfg.num_envs_per_env_runner
                )
                # Select proper number of evaluation workers for this round.
                selected_eval_worker_ids = [
                    worker_id
                    for i, worker_id in enumerate(
                        self.evaluation_workers.healthy_worker_ids()
                    )
                    if i * units_per_healthy_remote_worker < units_left_to_do
                ]
                self.evaluation_workers.foreach_worker_async(
                    func=lambda w: (w.sample(), w.get_metrics(), algo_iteration),
                    remote_worker_ids=selected_eval_worker_ids,
                )
                results = self.evaluation_workers.fetch_ready_async_reqs(
                    return_obj_refs=False, timeout_seconds=0.01
                )
                # Make sure we properly time out if we have not received any results
                # for more than `time_out` seconds.
                time_now = time.time()
                if not results and time_now - t_last_result > time_out:
                    break
                elif results:
                    t_last_result = time_now
                for wid, (batch, metrics, iter) in results:
                    if iter != self.iteration:
                        continue
                    env_steps += batch.env_steps()
                    agent_steps += batch.agent_steps()
                    all_metrics.extend(metrics)
                    if self.reward_estimators:
                        # TODO: (kourosh) This approach will cause an OOM issue when
                        #  the dataset gets huge (should be ok for now).
                        all_batches.append(batch)

                # 1 episode per returned batch.
                if unit == "episodes":
                    num_units_done += len(results)
                # n timesteps per returned batch.
                else:
                    num_units_done = (
                        env_steps
                        if self.config.count_steps_by == "env_steps"
                        else agent_steps
                    )

            # Update correct number of healthy remote workers.
            num_healthy_workers = self.evaluation_workers.num_healthy_remote_workers()

        if num_healthy_workers == 0:
            logger.warning(
                "Calling `sample()` on your remote evaluation worker(s) "
                "resulted in all workers crashing! Make sure a) your environment is not"
                " too unstable, b) you have enough evaluation workers "
                "(`config.evaluation(evaluation_num_env_runners=...)`) to cover for "
                "occasional losses, and c) you use the `config.fault_tolerance("
                "recreate_failed_env_runners=True)` setting."
            )

        if not self.config.enable_env_runner_and_connector_v2:
            env_runner_results = summarize_episodes(
                all_metrics,
                all_metrics,
                keep_custom_metrics=(
                    self.evaluation_config.keep_per_episode_custom_metrics
                ),
            )
            num_episodes = env_runner_results[NUM_EPISODES]
        else:
            self.metrics.log_n_dicts(
                all_metrics,
                key=(EVALUATION_RESULTS, ENV_RUNNER_RESULTS),
            )
            num_episodes = self.metrics.peek(
                (EVALUATION_RESULTS, ENV_RUNNER_RESULTS, NUM_EPISODES), default=0
            )
            env_runner_results = None

        # Warn if results are empty, it could be that this is because the eval timesteps
        # are not enough to run through one full episode.
        if num_episodes == 0:
            logger.warning(
                "This evaluation iteration resulted in an empty set of episode summary "
                "results! It's possible that your configured duration timesteps are not"
                " enough to finish even a single episode. Your have configured "
                f"{self.config.evaluation_duration}"
                f"{self.config.evaluation_duration_unit}. For 'timesteps', try "
                "increasing this value via the `config.evaluation(evaluation_duration="
                "...)` OR change the unit to 'episodes' via `config.evaluation("
                "evaluation_duration_unit='episodes')` OR try increasing the timeout "
                "threshold via `config.evaluation(evaluation_sample_timeout_s=...)` OR "
                "you can also set `config.evaluation_force_reset_envs_before_iteration`"
                " to False. However, keep in mind that in the latter case, the "
                "evaluation results may contain some episode stats generated with "
                "earlier weights versions."
            )

        return env_runner_results, env_steps, agent_steps, all_batches

    @OverrideToImplementCustomLogic
    @DeveloperAPI
    def restore_workers(self, workers: EnvRunnerGroup) -> None:
        """Try bringing back unhealthy EnvRunners and - if successful - sync with local.

        Algorithms that use custom EnvRunners may override this method to
        disable the default, and create custom restoration logics. Note that "restoring"
        does not include the actual restarting process, but merely what should happen
        after such a restart of a (previously failed) worker.

        Args:
            workers: The EnvRunnerGroup to restore. This may be the training or the
                evaluation EnvRunnerGroup.
        """
        # If `workers` is None, or
        # 1. `workers` (EnvRunnerGroup) does not have a local worker, and
        # 2. `self.workers` (EnvRunnerGroup used for training) does not have a local
        # worker -> we don't have a local worker to get state from, so we can't recover
        # remote worker in this case.
        if not workers or (
            not workers.local_worker() and not self.workers.local_worker()
        ):
            return

        # This is really cheap, since probe_unhealthy_workers() is a no-op
        # if there are no unhealthy workers.
        restored = workers.probe_unhealthy_workers()

        if restored:
            # Count the restored workers.
            self._counters["total_num_restored_workers"] += len(restored)

            from_worker = workers.local_worker() or self.workers.local_worker()
            # Get the state of the correct (reference) worker. E.g. The local worker
            # of the training EnvRunnerGroup.
            # TODO (sven): EnvRunners currently return an empty dict from `get_state()`.
            #  Once this API has been implemented properly (and replaced the
            #  `get/set_weights()` APIs), re-visit the test cases that check, whether
            #  all single-agent RLModules added on-the-fly are properly restored as
            #  well.
            state_ref = ray.put(from_worker.get_state())

            # By default, entire local worker state is synced after restoration
            # to bring these workers up to date.
            workers.foreach_worker(
                func=lambda w: w.set_state(ray.get(state_ref)),
                remote_worker_ids=restored,
                # Don't update the local_worker, b/c it's the one we are synching from.
                local_worker=False,
                timeout_seconds=self.config.env_runner_restore_timeout_s,
            )

            # Fire the callback for re-created workers.
            self.callbacks.on_workers_recreated(
                algorithm=self,
                worker_set=workers,
                worker_ids=restored,
                is_evaluation=workers.local_worker().config.in_evaluation,
            )

    @OverrideToImplementCustomLogic
    def training_step(self) -> ResultDict:
        """Default single iteration logic of an algorithm.

        - Collect on-policy samples (SampleBatches) in parallel using the
          Algorithm's EnvRunners (@ray.remote).
        - Concatenate collected SampleBatches into one train batch.
        - Note that we may have more than one policy in the multi-agent case:
          Call the different policies' `learn_on_batch` (simple optimizer) OR
          `load_batch_into_buffer` + `learn_on_loaded_batch` (multi-GPU
          optimizer) methods to calculate loss and update the model(s).
        - Return all collected metrics for the iteration.

        Returns:
            The results dict from executing the training iteration.
        """
        if not self.config.enable_env_runner_and_connector_v2:
            raise NotImplementedError(
                "The `Algorithm.training_step()` default implementation no longer "
                "supports the old or hybrid API stacks! If you would like to continue "
                "using these "
                "old APIs with this default `training_step`, simply subclass "
                "`Algorithm` and override its `training_step` method (copy/paste the "
                "code and delete this error message)."
            )

        # Collect SampleBatches from sample workers until we have a full batch.
        with self.metrics.log_time((TIMERS, ENV_RUNNER_SAMPLING_TIMER)):
            if self.config.count_steps_by == "agent_steps":
                train_batch, env_runner_results = synchronous_parallel_sample(
                    worker_set=self.workers,
                    max_agent_steps=self.config.train_batch_size,
                    sample_timeout_s=self.config.sample_timeout_s,
                    _uses_new_env_runners=(
                        self.config.enable_env_runner_and_connector_v2
                    ),
                    _return_metrics=True,
                )
            else:
                train_batch, env_runner_results = synchronous_parallel_sample(
                    worker_set=self.workers,
                    max_env_steps=self.config.train_batch_size,
                    sample_timeout_s=self.config.sample_timeout_s,
                    _uses_new_env_runners=(
                        self.config.enable_env_runner_and_connector_v2
                    ),
                    _return_metrics=True,
                )
        train_batch = train_batch.as_multi_agent()

        # Reduce EnvRunner metrics over the n EnvRunners.
        self.metrics.log_n_dicts(env_runner_results, key=ENV_RUNNER_RESULTS)

        # Only train if train_batch is not empty.
        # In an extreme situation, all rollout workers die during the
        # synchronous_parallel_sample() call above.
        # In which case, we should skip training, wait a little bit, then probe again.
        with self.metrics.log_time((TIMERS, LEARNER_UPDATE_TIMER)):
            if train_batch.agent_steps() > 0:
                learner_results = self.learner_group.update_from_batch(
                    batch=train_batch
                )
                self.metrics.log_dict(learner_results, key=LEARNER_RESULTS)
            else:
                # Wait 1 sec before probing again via weight syncing.
                time.sleep(1.0)

        # Update weights - after learning on the local worker - on all
        # remote workers (only those RLModules that were actually trained).
        with self.metrics.log_time((TIMERS, SYNCH_WORKER_WEIGHTS_TIMER)):
            self.workers.sync_weights(
                from_worker_or_learner_group=self.learner_group,
                policies=set(learner_results.keys()) - {ALL_MODULES},
                inference_only=True,
            )

        # Return reduced metrics (NestedDict).
        # Note that these training results will further be processed (e.g.
        # merged with evaluation results) before eventually being returned from the
        # encapsulating `Algorithm.step()` call as a plain nested ResultDict.
        return self.metrics.reduce()

    # TODO (sven): Deprecate this API in favor of extracting the correct RLModule
    #  and simply calling `forward_inference()` on it (see DreamerV3 for an example).
    @PublicAPI
    def compute_single_action(
        self,
        observation: Optional[TensorStructType] = None,
        state: Optional[List[TensorStructType]] = None,
        *,
        prev_action: Optional[TensorStructType] = None,
        prev_reward: Optional[float] = None,
        info: Optional[EnvInfoDict] = None,
        input_dict: Optional[SampleBatch] = None,
        policy_id: PolicyID = DEFAULT_POLICY_ID,
        full_fetch: bool = False,
        explore: Optional[bool] = None,
        timestep: Optional[int] = None,
        episode: Optional[Episode] = None,
        unsquash_action: Optional[bool] = None,
        clip_action: Optional[bool] = None,
        # Kwargs placeholder for future compatibility.
        **kwargs,
    ) -> Union[
        TensorStructType,
        Tuple[TensorStructType, List[TensorType], Dict[str, TensorType]],
    ]:
        """Computes an action for the specified policy on the local worker.

        Note that you can also access the policy object through
        self.get_policy(policy_id) and call compute_single_action() on it
        directly.

        Args:
            observation: Single (unbatched) observation from the
                environment.
            state: List of all RNN hidden (single, unbatched) state tensors.
            prev_action: Single (unbatched) previous action value.
            prev_reward: Single (unbatched) previous reward value.
            info: Env info dict, if any.
            input_dict: An optional SampleBatch that holds all the values
                for: obs, state, prev_action, and prev_reward, plus maybe
                custom defined views of the current env trajectory. Note
                that only one of `obs` or `input_dict` must be non-None.
            policy_id: Policy to query (only applies to multi-agent).
                Default: "default_policy".
            full_fetch: Whether to return extra action fetch results.
                This is always set to True if `state` is specified.
            explore: Whether to apply exploration to the action.
                Default: None -> use self.config.explore.
            timestep: The current (sampling) time step.
            episode: This provides access to all of the internal episodes'
                state, which may be useful for model-based or multi-agent
                algorithms.
            unsquash_action: Should actions be unsquashed according to the
                env's/Policy's action space? If None, use the value of
                self.config.normalize_actions.
            clip_action: Should actions be clipped according to the
                env's/Policy's action space? If None, use the value of
                self.config.clip_actions.

        Keyword Args:
            kwargs: forward compatibility placeholder

        Returns:
            The computed action if full_fetch=False, or a tuple of a) the
            full output of policy.compute_actions() if full_fetch=True
            or we have an RNN-based Policy.

        Raises:
            KeyError: If the `policy_id` cannot be found in this Algorithm's local
                worker.
        """
        # `unsquash_action` is None: Use value of config['normalize_actions'].
        if unsquash_action is None:
            unsquash_action = self.config.normalize_actions
        # `clip_action` is None: Use value of config['clip_actions'].
        elif clip_action is None:
            clip_action = self.config.clip_actions

        # User provided an input-dict: Assert that `obs`, `prev_a|r`, `state`
        # are all None.
        err_msg = (
            "Provide either `input_dict` OR [`observation`, ...] as "
            "args to `Algorithm.compute_single_action()`!"
        )
        if input_dict is not None:
            assert (
                observation is None
                and prev_action is None
                and prev_reward is None
                and state is None
            ), err_msg
            observation = input_dict[Columns.OBS]
        else:
            assert observation is not None, err_msg

        # Get the policy to compute the action for (in the multi-agent case,
        # Algorithm may hold >1 policies).
        policy = self.get_policy(policy_id)
        if policy is None:
            raise KeyError(
                f"PolicyID '{policy_id}' not found in PolicyMap of the "
                f"Algorithm's local worker!"
            )
        local_worker = self.workers.local_worker()

        if not self.config.get("enable_connectors"):
            # Check the preprocessor and preprocess, if necessary.
            pp = local_worker.preprocessors[policy_id]
            if pp and type(pp).__name__ != "NoPreprocessor":
                observation = pp.transform(observation)
            observation = local_worker.filters[policy_id](observation, update=False)
        else:
            # Just preprocess observations, similar to how it used to be done before.
            pp = policy.agent_connectors[ObsPreprocessorConnector]

            # convert the observation to array if possible
            if not isinstance(observation, (np.ndarray, dict, tuple)):
                try:
                    observation = np.asarray(observation)
                except Exception:
                    raise ValueError(
                        f"Observation type {type(observation)} cannot be converted to "
                        f"np.ndarray."
                    )
            if pp:
                assert len(pp) == 1, "Only one preprocessor should be in the pipeline"
                pp = pp[0]

                if not pp.is_identity():
                    # Note(Kourosh): This call will leave the policy's connector
                    # in eval mode. would that be a problem?
                    pp.in_eval()
                    if observation is not None:
                        _input_dict = {Columns.OBS: observation}
                    elif input_dict is not None:
                        _input_dict = {Columns.OBS: input_dict[Columns.OBS]}
                    else:
                        raise ValueError(
                            "Either observation or input_dict must be provided."
                        )

                    # TODO (Kourosh): Create a new util method for algorithm that
                    # computes actions based on raw inputs from env and can keep track
                    # of its own internal state.
                    acd = AgentConnectorDataType("0", "0", _input_dict)
                    # make sure the state is reset since we are only applying the
                    # preprocessor
                    pp.reset(env_id="0")
                    ac_o = pp([acd])[0]
                    observation = ac_o.data[Columns.OBS]

        # Input-dict.
        if input_dict is not None:
            input_dict[Columns.OBS] = observation
            action, state, extra = policy.compute_single_action(
                input_dict=input_dict,
                explore=explore,
                timestep=timestep,
                episode=episode,
            )
        # Individual args.
        else:
            action, state, extra = policy.compute_single_action(
                obs=observation,
                state=state,
                prev_action=prev_action,
                prev_reward=prev_reward,
                info=info,
                explore=explore,
                timestep=timestep,
                episode=episode,
            )

        # If we work in normalized action space (normalize_actions=True),
        # we re-translate here into the env's action space.
        if unsquash_action:
            action = space_utils.unsquash_action(action, policy.action_space_struct)
        # Clip, according to env's action space.
        elif clip_action:
            action = space_utils.clip_action(action, policy.action_space_struct)

        # Return 3-Tuple: Action, states, and extra-action fetches.
        if state or full_fetch:
            return action, state, extra
        # Ensure backward compatibility.
        else:
            return action

    @PublicAPI
    def compute_actions(
        self,
        observations: TensorStructType,
        state: Optional[List[TensorStructType]] = None,
        *,
        prev_action: Optional[TensorStructType] = None,
        prev_reward: Optional[TensorStructType] = None,
        info: Optional[EnvInfoDict] = None,
        policy_id: PolicyID = DEFAULT_POLICY_ID,
        full_fetch: bool = False,
        explore: Optional[bool] = None,
        timestep: Optional[int] = None,
        episodes: Optional[List[Episode]] = None,
        unsquash_actions: Optional[bool] = None,
        clip_actions: Optional[bool] = None,
        **kwargs,
    ):
        """Computes an action for the specified policy on the local Worker.

        Note that you can also access the policy object through
        self.get_policy(policy_id) and call compute_actions() on it directly.

        Args:
            observation: Observation from the environment.
            state: RNN hidden state, if any. If state is not None,
                then all of compute_single_action(...) is returned
                (computed action, rnn state(s), logits dictionary).
                Otherwise compute_single_action(...)[0] is returned
                (computed action).
            prev_action: Previous action value, if any.
            prev_reward: Previous reward, if any.
            info: Env info dict, if any.
            policy_id: Policy to query (only applies to multi-agent).
            full_fetch: Whether to return extra action fetch results.
                This is always set to True if RNN state is specified.
            explore: Whether to pick an exploitation or exploration
                action (default: None -> use self.config.explore).
            timestep: The current (sampling) time step.
            episodes: This provides access to all of the internal episodes'
                state, which may be useful for model-based or multi-agent
                algorithms.
            unsquash_actions: Should actions be unsquashed according
                to the env's/Policy's action space? If None, use
                self.config.normalize_actions.
            clip_actions: Should actions be clipped according to the
                env's/Policy's action space? If None, use
                self.config.clip_actions.

        Keyword Args:
            kwargs: forward compatibility placeholder

        Returns:
            The computed action if full_fetch=False, or a tuple consisting of
            the full output of policy.compute_actions_from_input_dict() if
            full_fetch=True or we have an RNN-based Policy.
        """
        # `unsquash_actions` is None: Use value of config['normalize_actions'].
        if unsquash_actions is None:
            unsquash_actions = self.config.normalize_actions
        # `clip_actions` is None: Use value of config['clip_actions'].
        elif clip_actions is None:
            clip_actions = self.config.clip_actions

        # Preprocess obs and states.
        state_defined = state is not None
        policy = self.get_policy(policy_id)
        filtered_obs, filtered_state = [], []
        for agent_id, ob in observations.items():
            worker = self.workers.local_worker()
            if worker.preprocessors.get(policy_id) is not None:
                preprocessed = worker.preprocessors[policy_id].transform(ob)
            else:
                preprocessed = ob
            filtered = worker.filters[policy_id](preprocessed, update=False)
            filtered_obs.append(filtered)
            if state is None:
                continue
            elif agent_id in state:
                filtered_state.append(state[agent_id])
            else:
                filtered_state.append(policy.get_initial_state())

        # Batch obs and states
        obs_batch = np.stack(filtered_obs)
        if state is None:
            state = []
        else:
            state = list(zip(*filtered_state))
            state = [np.stack(s) for s in state]

        input_dict = {Columns.OBS: obs_batch}

        # prev_action and prev_reward can be None, np.ndarray, or tensor-like structure.
        # Explicitly check for None here to avoid the error message "The truth value of
        # an array with more than one element is ambiguous.", when np arrays are passed
        # as arguments.
        if prev_action is not None:
            input_dict[SampleBatch.PREV_ACTIONS] = prev_action
        if prev_reward is not None:
            input_dict[SampleBatch.PREV_REWARDS] = prev_reward
        if info:
            input_dict[Columns.INFOS] = info
        for i, s in enumerate(state):
            input_dict[f"state_in_{i}"] = s

        # Batch compute actions
        actions, states, infos = policy.compute_actions_from_input_dict(
            input_dict=input_dict,
            explore=explore,
            timestep=timestep,
            episodes=episodes,
        )

        # Unbatch actions for the environment into a multi-agent dict.
        single_actions = space_utils.unbatch(actions)
        actions = {}
        for key, a in zip(observations, single_actions):
            # If we work in normalized action space (normalize_actions=True),
            # we re-translate here into the env's action space.
            if unsquash_actions:
                a = space_utils.unsquash_action(a, policy.action_space_struct)
            # Clip, according to env's action space.
            elif clip_actions:
                a = space_utils.clip_action(a, policy.action_space_struct)
            actions[key] = a

        # Unbatch states into a multi-agent dict.
        unbatched_states = {}
        for idx, agent_id in enumerate(observations):
            unbatched_states[agent_id] = [s[idx] for s in states]

        # Return only actions or full tuple
        if state_defined or full_fetch:
            return actions, unbatched_states, infos
        else:
            return actions

    @PublicAPI
    def get_module(self, module_id: ModuleID = DEFAULT_MODULE_ID) -> RLModule:
        """Returns the (single-agent) RLModule with `model_id` (None if ID not found).

        Args:
            module_id: ID of the (single-agent) RLModule to return from the MARLModule
                used by the local EnvRunner.

        Returns:
            The SingleAgentRLModule sitting under the ModuleID key inside the
            local worker's (EnvRunner's) MARLModule.
        """
        return self.workers.local_worker().module[module_id]

    @PublicAPI
    def get_policy(self, policy_id: PolicyID = DEFAULT_POLICY_ID) -> Policy:
        """Return policy for the specified id, or None.

        Args:
            policy_id: ID of the policy to return.
        """
        return self.workers.local_worker().get_policy(policy_id)

    @PublicAPI
    def get_weights(self, policies: Optional[List[PolicyID]] = None) -> dict:
        """Return a dictionary of policy ids to weights.

        Args:
            policies: Optional list of policies to return weights for,
                or None for all policies.
        """
        return self.workers.local_worker().get_weights(policies)

    @PublicAPI
    def set_weights(self, weights: Dict[PolicyID, dict]):
        """Set policy weights by policy id.

        Args:
            weights: Map of policy ids to weights to set.
        """
        self.workers.local_worker().set_weights(weights)

    @PublicAPI
    def add_policy(
        self,
        policy_id: PolicyID,
        policy_cls: Optional[Type[Policy]] = None,
        policy: Optional[Policy] = None,
        *,
        observation_space: Optional[gym.spaces.Space] = None,
        action_space: Optional[gym.spaces.Space] = None,
        config: Optional[Union[AlgorithmConfig, PartialAlgorithmConfigDict]] = None,
        policy_state: Optional[PolicyState] = None,
        policy_mapping_fn: Optional[Callable[[AgentID, EpisodeID], PolicyID]] = None,
        policies_to_train: Optional[
            Union[
                Container[PolicyID],
                Callable[[PolicyID, Optional[SampleBatchType]], bool],
            ]
        ] = None,
        evaluation_workers: bool = True,
        module_spec: Optional[SingleAgentRLModuleSpec] = None,
    ) -> Optional[Policy]:
        """Adds a new policy to this Algorithm.

        Args:
            policy_id: ID of the policy to add.
                IMPORTANT: Must not contain characters that
                are also not allowed in Unix/Win filesystems, such as: `<>:"/|?*`,
                or a dot, space or backslash at the end of the ID.
            policy_cls: The Policy class to use for constructing the new Policy.
                Note: Only one of `policy_cls` or `policy` must be provided.
            policy: The Policy instance to add to this algorithm. If not None, the
                given Policy object will be directly inserted into the Algorithm's
                local worker and clones of that Policy will be created on all remote
                workers as well as all evaluation workers.
                Note: Only one of `policy_cls` or `policy` must be provided.
            observation_space: The observation space of the policy to add.
                If None, try to infer this space from the environment.
            action_space: The action space of the policy to add.
                If None, try to infer this space from the environment.
            config: The config object or overrides for the policy to add.
            policy_state: Optional state dict to apply to the new
                policy instance, right after its construction.
            policy_mapping_fn: An optional (updated) policy mapping function
                to use from here on. Note that already ongoing episodes will
                not change their mapping but will use the old mapping till
                the end of the episode.
            policies_to_train: An optional list of policy IDs to be trained
                or a callable taking PolicyID and SampleBatchType and
                returning a bool (trainable or not?).
                If None, will keep the existing setup in place. Policies,
                whose IDs are not in the list (or for which the callable
                returns False) will not be updated.
            evaluation_workers: Whether to add the new policy also
                to the evaluation EnvRunnerGroup.
            module_spec: In the new RLModule API we need to pass in the module_spec for
                the new module that is supposed to be added. Knowing the policy spec is
                not sufficient.

        Returns:
            The newly added policy (the copy that got added to the local
            worker). If `workers` was provided, None is returned.
        """
        if self.config.enable_env_runner_and_connector_v2:
            raise ValueError(
                "`Algorithm.add_policy()` is not supported on the new API stack w/ "
                "EnvRunners! Use `Algorithm.add_module()` instead. Also see "
                "`rllib/examples/self_play_league_based_with_open_spiel.py` for an "
                "example."
            )

        validate_policy_id(policy_id, error=True)

        self.workers.add_policy(
            policy_id,
            policy_cls,
            policy,
            observation_space=observation_space,
            action_space=action_space,
            config=config,
            policy_state=policy_state,
            policy_mapping_fn=policy_mapping_fn,
            policies_to_train=policies_to_train,
            module_spec=module_spec,
        )

        # If learner API is enabled, we need to also add the underlying module
        # to the learner group.
        if self.config.enable_rl_module_and_learner:
            policy = self.get_policy(policy_id)
            module = policy.model
            self.learner_group.add_module(
                module_id=policy_id,
                module_spec=SingleAgentRLModuleSpec.from_module(module),
            )

            # Update each Learner's `policies_to_train` information, but only
            # if the arg is explicitly provided here.
            if policies_to_train is not None:
                self.learner_group.foreach_learner(
                    lambda learner: learner.config.multi_agent(
                        policies_to_train=policies_to_train
                    )
                )

            weights = policy.get_weights()
            self.learner_group.set_weights({policy_id: weights})

        # Add to evaluation workers, if necessary.
        if evaluation_workers is True and self.evaluation_workers is not None:
            self.evaluation_workers.add_policy(
                policy_id,
                policy_cls,
                policy,
                observation_space=observation_space,
                action_space=action_space,
                config=config,
                policy_state=policy_state,
                policy_mapping_fn=policy_mapping_fn,
                policies_to_train=policies_to_train,
                module_spec=module_spec,
            )

        # Return newly added policy (from the local rollout worker).
        return self.get_policy(policy_id)

    @PublicAPI
    def add_module(
        self,
        module_id: ModuleID,
        module_spec: SingleAgentRLModuleSpec,
        *,
        module_state: Optional[Dict] = None,
        new_agent_to_module_mapping_fn: Optional[AgentToModuleMappingFn] = None,
        new_should_module_be_updated: Optional[ShouldModuleBeUpdatedFn] = None,
        evaluation_workers: bool = True,
    ) -> Optional[Policy]:
        """Adds a new (single-agent) RLModule to this Algorithm's MARLModule.

        Args:
            module_id: ID of the RLModule to add to the MARLModule.
                IMPORTANT: Must not contain characters that
                are also not allowed in Unix/Win filesystems, such as: `<>:"/|?*`,
                or a dot, space or backslash at the end of the ID.
            module_spec: The SingleAgentRLModuleSpec to use for constructing the new
                RLModule.
            module_state: Optional state dict to apply to the new
                RLModule instance, right after its construction.
            new_agent_to_module_mapping_fn: An optional (updated) AgentID to ModuleID
                mapping function to use from here on. Note that already ongoing
                episodes will not change their mapping but will use the old mapping till
                the end of the episode.
            new_should_module_be_updated: An optional sequence of ModuleIDs or a
                callable taking ModuleID and SampleBatchType and returning whether the
                ModuleID should be updated (trained).
                If None, will keep the existing setup in place. RLModules,
                whose IDs are not in the list (or for which the callable
                returns False) will not be updated.
            evaluation_workers: Whether to add the new RLModule also
                to the evaluation EnvRunnerGroup.

        Returns:
            The newly added RLModule (the copy that got added to the local
            worker).
        """
        validate_policy_id(module_id, error=True)

        def _add(env_runner_or_learner):
            env_runner_or_learner.module.add_module(
                module_id=module_id, module=module_spec.build()
            )
            if new_agent_to_module_mapping_fn is not None:
                env_runner_or_learner.config.multi_agent(
                    policy_mapping_fn=new_agent_to_module_mapping_fn
                )
            # This setting doesn't really matter for EnvRunners (no
            # training going on there, but we'll update this as well
            # here for good measure).
            if new_should_module_be_updated is not None:
                env_runner_or_learner.config.multi_agent(
                    policies_to_train=new_should_module_be_updated
                )

        # Create RLModule on all EnvRunners.
        self.workers.foreach_worker(_add, local_worker=True)
        self.workers.sync_weights(policies=[module_id], inference_only=True)
        # Also on the eval EnvRunners?
        if evaluation_workers is True and self.evaluation_workers is not None:
            self.evaluation_workers.foreach_worker(_add, local_worker=True)
            self.evaluation_workers.sync_weights(
                policies=[module_id], inference_only=True
            )
        # Create RLModule on all Learner workers.
        new_module = self.workers.local_worker().module[module_id]
        self.learner_group.foreach_learner(_add)
        self.learner_group.set_weights({module_id: new_module.get_state()})

        # Return newly added RLModule (from the local EnvRunner).
        return new_module

    @PublicAPI
    def remove_policy(
        self,
        policy_id: PolicyID = DEFAULT_POLICY_ID,
        *,
        policy_mapping_fn: Optional[Callable[[AgentID], PolicyID]] = None,
        policies_to_train: Optional[
            Union[
                Container[PolicyID],
                Callable[[PolicyID, Optional[SampleBatchType]], bool],
            ]
        ] = None,
        evaluation_workers: bool = True,
    ) -> None:
        """Removes a new policy from this Algorithm.

        Args:
            policy_id: ID of the policy to be removed.
            policy_mapping_fn: An optional (updated) policy mapping function
                to use from here on. Note that already ongoing episodes will
                not change their mapping but will use the old mapping till
                the end of the episode.
            policies_to_train: An optional list of policy IDs to be trained
                or a callable taking PolicyID and SampleBatchType and
                returning a bool (trainable or not?).
                If None, will keep the existing setup in place. Policies,
                whose IDs are not in the list (or for which the callable
                returns False) will not be updated.
            evaluation_workers: Whether to also remove the policy from the
                evaluation EnvRunnerGroup.
        """

        def fn(worker):
            worker.remove_policy(
                policy_id=policy_id,
                policy_mapping_fn=policy_mapping_fn,
                policies_to_train=policies_to_train,
            )

        # Update all EnvRunner workers.
        self.workers.foreach_worker(fn, local_worker=True)

        # Update each Learner's `policies_to_train` information, but only
        # if the arg is explicitly provided here.
        if self.config.enable_rl_module_and_learner and policies_to_train is not None:
            self.learner_group.foreach_learner(
                lambda learner: learner.config.multi_agent(
                    policies_to_train=policies_to_train
                )
            )

        # Update the evaluation worker set's workers, if required.
        if evaluation_workers and self.evaluation_workers is not None:
            self.evaluation_workers.foreach_worker(fn, local_worker=True)

    @OldAPIStack
    def export_policy_model(
        self,
        export_dir: str,
        policy_id: PolicyID = DEFAULT_POLICY_ID,
        onnx: Optional[int] = None,
    ) -> None:
        """Exports policy model with given policy_id to a local directory.

        Args:
            export_dir: Writable local directory.
            policy_id: Optional policy id to export.
            onnx: If given, will export model in ONNX format. The
                value of this parameter set the ONNX OpSet version to use.
                If None, the output format will be DL framework specific.

        .. testcode::

            from ray.rllib.algorithms.ppo import PPO, PPOConfig
            config = PPOConfig().environment("CartPole-v1")
            algo = PPO(config=config)
            algo.train()
            algo.export_policy_checkpoint("/tmp/export_dir")
            algo.export_policy_model("/tmp/dir")
        """
        self.get_policy(policy_id).export_model(export_dir, onnx)

    @OldAPIStack
    def export_policy_checkpoint(
        self,
        export_dir: str,
        policy_id: PolicyID = DEFAULT_POLICY_ID,
    ) -> None:
        """Exports Policy checkpoint to a local directory and returns an AIR Checkpoint.

        Args:
            export_dir: Writable local directory to store the AIR Checkpoint
                information into.
            policy_id: Optional policy ID to export. If not provided, will export
                "default_policy". If `policy_id` does not exist in this Algorithm,
                will raise a KeyError.

        Raises:
            KeyError if `policy_id` cannot be found in this Algorithm.

        .. testcode::

            from ray.rllib.algorithms.ppo import PPO, PPOConfig
            config = PPOConfig().environment("CartPole-v1")
            algo = PPO(config=config)
            algo.train()
            algo.export_policy_checkpoint("/tmp/export_dir")
        """
        policy = self.get_policy(policy_id)
        if policy is None:
            raise KeyError(f"Policy with ID {policy_id} not found in Algorithm!")
        policy.export_checkpoint(export_dir)

    @override(Trainable)
    def save_checkpoint(self, checkpoint_dir: str) -> None:
        """Exports checkpoint to a local directory.

        The structure of an Algorithm checkpoint dir will be as follows::

            policies/
                pol_1/
                    policy_state.pkl
                pol_2/
                    policy_state.pkl
            learner/
                learner_state.json
                module_state/
                    module_1/
                        ...
                optimizer_state/
                    optimizers_module_1/
                        ...
            rllib_checkpoint.json
            algorithm_state.pkl

        Note: `rllib_checkpoint.json` contains a "version" key (e.g. with value 0.1)
        helping RLlib to remain backward compatible wrt. restoring from checkpoints from
        Ray 2.0 onwards.

        Args:
            checkpoint_dir: The directory where the checkpoint files will be stored.
        """
        state = self.__getstate__()

        # TODO (sven): Move LearnerGroup `get_state` call here as well.

        # Extract policy states from worker state (Policies get their own
        # checkpoint sub-dirs).
        policy_states = {}
        if "worker" in state and "policy_states" in state["worker"]:
            policy_states = state["worker"].pop("policy_states", {})

        # Add RLlib checkpoint version.
        if self.config.enable_rl_module_and_learner:
            state["checkpoint_version"] = CHECKPOINT_VERSION_LEARNER
        else:
            state["checkpoint_version"] = CHECKPOINT_VERSION

        # Write state (w/o policies) to disk.
        state_file = os.path.join(checkpoint_dir, "algorithm_state.pkl")
        with open(state_file, "wb") as f:
            pickle.dump(state, f)

        # Write rllib_checkpoint.json.
        with open(os.path.join(checkpoint_dir, "rllib_checkpoint.json"), "w") as f:
            json.dump(
                {
                    "type": "Algorithm",
                    "checkpoint_version": str(state["checkpoint_version"]),
                    "format": "cloudpickle",
                    "state_file": state_file,
                    "policy_ids": list(policy_states.keys()),
                    "ray_version": ray.__version__,
                    "ray_commit": ray.__commit__,
                },
                f,
            )

        # Write individual policies to disk, each in their own sub-directory.
        for pid, policy_state in policy_states.items():
            # From here on, disallow policyIDs that would not work as directory names.
            validate_policy_id(pid, error=True)
            policy_dir = os.path.join(checkpoint_dir, "policies", pid)
            os.makedirs(policy_dir, exist_ok=True)
            policy = self.get_policy(pid)
            policy.export_checkpoint(policy_dir, policy_state=policy_state)

        # if we are using the learner API, save the learner group state
        if self.config.enable_rl_module_and_learner:
            learner_state_dir = os.path.join(checkpoint_dir, "learner")
            self.learner_group.save_state(learner_state_dir)

    @override(Trainable)
    def load_checkpoint(self, checkpoint_dir: str) -> None:
        # Checkpoint is provided as a local directory.
        # Restore from the checkpoint file or dir.

        checkpoint_info = get_checkpoint_info(checkpoint_dir)
        checkpoint_data = Algorithm._checkpoint_info_to_algorithm_state(checkpoint_info)
        self.__setstate__(checkpoint_data)
        if self.config.enable_rl_module_and_learner:
            learner_state_dir = os.path.join(checkpoint_dir, "learner")
            self.learner_group.load_state(learner_state_dir)
            # Make also sure, all training EnvRunners get the just loaded weights.
            weights = self.learner_group.get_weights(
                inference_only=self.config.enable_env_runner_and_connector_v2
            )
            self.workers.local_worker().set_weights(weights)
            self.workers.sync_weights(inference_only=True)

        # Call the `on_checkpoint_loaded` callback.
        self.callbacks.on_checkpoint_loaded(algorithm=self)

    @override(Trainable)
    def log_result(self, result: ResultDict) -> None:
        # Log after the callback is invoked, so that the user has a chance
        # to mutate the result.
        # TODO (sven): It might not make sense to pass in the MetricsLogger at this late
        #  point in time. In here, the result dict has already been "compiled" (reduced)
        #  by the MetricsLogger and there is probably no point in adding more Stats
        #  here.
        self.callbacks.on_train_result(
            algorithm=self, metrics_logger=self.metrics, result=result
        )
        # Then log according to Trainable's logging logic.
        Trainable.log_result(self, result)

    @override(Trainable)
    def cleanup(self) -> None:
        # Stop all workers.
        if hasattr(self, "workers") and self.workers is not None:
            self.workers.stop()
        if hasattr(self, "evaluation_workers") and self.evaluation_workers is not None:
            self.evaluation_workers.stop()

    @OverrideToImplementCustomLogic
    @classmethod
    @override(Trainable)
    def default_resource_request(
        cls, config: Union[AlgorithmConfig, PartialAlgorithmConfigDict]
    ) -> Union[Resources, PlacementGroupFactory]:
        # Default logic for RLlib Algorithms:
        # Create one bundle per individual worker (local or remote).
        # Use `num_cpus_for_local_worker` and `num_gpus` for the local worker and
        # `num_cpus_per_worker` and `num_gpus_per_worker` for the remote
        # workers to determine their CPU/GPU resource needs.

        # Convenience config handles.
        cf = cls.get_default_config().update_from_dict(config)
        cf.validate()
        cf.freeze()

        # get evaluation config
        eval_cf = cf.get_evaluation_config_object()
        eval_cf.validate()
        eval_cf.freeze()

        # resources for the driver of this trainable
        if cf.enable_rl_module_and_learner:
            if cf.num_learner_workers == 0:
                # in this case local_worker only does sampling and training is done on
                # local learner worker
                driver = cls._get_learner_bundles(cf)[0]
            else:
                # in this case local_worker only does sampling and training is done on
                # remote learner workers
                driver = {"CPU": cf.num_cpus_for_local_worker, "GPU": 0}
        else:
            driver = {
                "CPU": cf.num_cpus_for_local_worker,
                "GPU": 0 if cf._fake_gpus else cf.num_gpus,
            }

        # resources for remote rollout env samplers
        rollout_bundles = [
            {
                "CPU": cf.num_cpus_per_worker,
                "GPU": cf.num_gpus_per_worker,
                **cf.custom_resources_per_worker,
            }
            for _ in range(cf.num_env_runners)
        ]

        # resources for remote evaluation env samplers or datasets (if any)
        if cls._should_create_evaluation_rollout_workers(eval_cf):
            # Evaluation workers.
            # Note: The local eval worker is located on the driver CPU.
            evaluation_bundles = [
                {
                    "CPU": eval_cf.num_cpus_per_worker,
                    "GPU": eval_cf.num_gpus_per_worker,
                    **eval_cf.custom_resources_per_worker,
                }
                for _ in range(eval_cf.evaluation_num_env_runners)
            ]
        else:
            # resources for offline dataset readers during evaluation
            # Note (Kourosh): we should not claim extra workers for
            # training on the offline dataset, since rollout workers have already
            # claimed it.
            # Another Note (Kourosh): dataset reader will not use placement groups so
            # whatever we specify here won't matter because dataset won't even use it.
            # Disclaimer: using ray dataset in tune may cause deadlock when multiple
            # tune trials get scheduled on the same node and do not leave any spare
            # resources for dataset operations. The workaround is to limit the
            # max_concurrent trials so that some spare cpus are left for dataset
            # operations. This behavior should get fixed by the dataset team. more info
            # found here:
            # https://docs.ray.io/en/master/data/dataset-internals.html#datasets-tune
            evaluation_bundles = []

        # resources for remote learner workers
        learner_bundles = []
        if cf.enable_rl_module_and_learner and cf.num_learner_workers > 0:
            learner_bundles = cls._get_learner_bundles(cf)

        bundles = [driver] + rollout_bundles + evaluation_bundles + learner_bundles

        # Return PlacementGroupFactory containing all needed resources
        # (already properly defined as device bundles).
        return PlacementGroupFactory(
            bundles=bundles,
            strategy=config.get("placement_strategy", "PACK"),
        )

    @DeveloperAPI
    def _before_evaluate(self):
        """Pre-evaluation callback."""
        pass

    @staticmethod
    def _get_env_id_and_creator(
        env_specifier: Union[str, EnvType, None], config: AlgorithmConfig
    ) -> Tuple[Optional[str], EnvCreator]:
        """Returns env_id and creator callable given original env id from config.

        Args:
            env_specifier: An env class, an already tune registered env ID, a known
                gym env name, or None (if no env is used).
            config: The AlgorithmConfig object.

        Returns:
            Tuple consisting of a) env ID string and b) env creator callable.
        """
        # Environment is specified via a string.
        if isinstance(env_specifier, str):
            # An already registered env.
            if _global_registry.contains(ENV_CREATOR, env_specifier):
                return env_specifier, _global_registry.get(ENV_CREATOR, env_specifier)

            # A class path specifier.
            elif "." in env_specifier:

                def env_creator_from_classpath(env_context):
                    try:
                        env_obj = from_config(env_specifier, env_context)
                    except ValueError:
                        raise EnvError(
                            ERR_MSG_INVALID_ENV_DESCRIPTOR.format(env_specifier)
                        )
                    return env_obj

                return env_specifier, env_creator_from_classpath
            # Try gym/PyBullet.
            else:
                return env_specifier, functools.partial(
                    _gym_env_creator, env_descriptor=env_specifier
                )

        elif isinstance(env_specifier, type):
            env_id = env_specifier  # .__name__

            if config["remote_worker_envs"]:
                # Check gym version (0.22 or higher?).
                # If > 0.21, can't perform auto-wrapping of the given class as this
                # would lead to a pickle error.
                gym_version = importlib.metadata.version("gym")
                if version.parse(gym_version) >= version.parse("0.22"):
                    raise ValueError(
                        "Cannot specify a gym.Env class via `config.env` while setting "
                        "`config.remote_worker_env=True` AND your gym version is >= "
                        "0.22! Try installing an older version of gym or set `config."
                        "remote_worker_env=False`."
                    )

                @ray.remote(num_cpus=1)
                class _wrapper(env_specifier):
                    # Add convenience `_get_spaces` and `_is_multi_agent`
                    # methods:
                    def _get_spaces(self):
                        return self.observation_space, self.action_space

                    def _is_multi_agent(self):
                        from ray.rllib.env.multi_agent_env import MultiAgentEnv

                        return isinstance(self, MultiAgentEnv)

                return env_id, lambda cfg: _wrapper.remote(cfg)
            # gym.Env-subclass: Also go through our RLlib gym-creator.
            elif issubclass(env_specifier, gym.Env):
                return env_id, functools.partial(
                    _gym_env_creator,
                    env_descriptor=env_specifier,
                )
            # All other env classes: Call c'tor directly.
            else:
                return env_id, lambda cfg: env_specifier(cfg)

        # No env -> Env creator always returns None.
        elif env_specifier is None:
            return None, lambda env_config: None

        else:
            raise ValueError(
                "{} is an invalid env specifier. ".format(env_specifier)
                + "You can specify a custom env as either a class "
                '(e.g., YourEnvCls) or a registered env id (e.g., "your_env").'
            )

    def _sync_filters_if_needed(
        self,
        *,
        central_worker: EnvRunner,
        workers: EnvRunnerGroup,
        config: AlgorithmConfig,
    ) -> None:
        """Synchronizes the filter stats from `workers` to `central_worker`.

        .. and broadcasts the central_worker's filter stats back to all `workers`
        (if configured).

        Args:
            central_worker: The worker to sync/aggregate all `workers`' filter stats to
                and from which to (possibly) broadcast the updated filter stats back to
                `workers`.
            workers: The EnvRunnerGroup, whose EnvRunners' filter stats should be used
                for aggregation on `central_worker` and which (possibly) get updated
                from `central_worker` after the sync.
            config: The algorithm config instance. This is used to determine, whether
                syncing from `workers` should happen at all and whether broadcasting
                back to `workers` (after possible syncing) should happen.
        """
        if central_worker and config.observation_filter != "NoFilter":
            FilterManager.synchronize(
                central_worker.filters,
                workers,
                update_remote=config.update_worker_filter_stats,
                timeout_seconds=config.sync_filters_on_rollout_workers_timeout_s,
                use_remote_data_for_update=config.use_worker_filter_stats,
            )

    @classmethod
    @override(Trainable)
    def resource_help(cls, config: Union[AlgorithmConfig, AlgorithmConfigDict]) -> str:
        return (
            "\n\nYou can adjust the resource requests of RLlib Algorithms by calling "
            "`AlgorithmConfig.resources("
            "num_gpus=.., num_cpus_per_worker=.., num_gpus_per_worker=.., ..)` or "
            "`AgorithmConfig.env_runners(num_env_runners=..)`. See "
            "the `ray.rllib.algorithms.algorithm_config.AlgorithmConfig` classes "
            "(each Algorithm has its own subclass of this class) for more info.\n\n"
            f"The config of this Algorithm is: {config}"
        )

    @override(Trainable)
    def get_auto_filled_metrics(
        self,
        now: Optional[datetime] = None,
        time_this_iter: Optional[float] = None,
        timestamp: Optional[int] = None,
        debug_metrics_only: bool = False,
    ) -> dict:
        # Override this method to make sure, the `config` key of the returned results
        # contains the proper Tune config dict (instead of an AlgorithmConfig object).
        auto_filled = super().get_auto_filled_metrics(
            now, time_this_iter, timestamp, debug_metrics_only
        )
        if "config" not in auto_filled:
            raise KeyError("`config` key not found in auto-filled results dict!")

        # If `config` key is no dict (but AlgorithmConfig object) ->
        # make sure, it's a dict to not break Tune APIs.
        if not isinstance(auto_filled["config"], dict):
            assert isinstance(auto_filled["config"], AlgorithmConfig)
            auto_filled["config"] = auto_filled["config"].to_dict()
        return auto_filled

    @classmethod
    def merge_algorithm_configs(
        cls,
        config1: AlgorithmConfigDict,
        config2: PartialAlgorithmConfigDict,
        _allow_unknown_configs: Optional[bool] = None,
    ) -> AlgorithmConfigDict:
        """Merges a complete Algorithm config dict with a partial override dict.

        Respects nested structures within the config dicts. The values in the
        partial override dict take priority.

        Args:
            config1: The complete Algorithm's dict to be merged (overridden)
                with `config2`.
            config2: The partial override config dict to merge on top of
                `config1`.
            _allow_unknown_configs: If True, keys in `config2` that don't exist
                in `config1` are allowed and will be added to the final config.

        Returns:
            The merged full algorithm config dict.
        """
        config1 = copy.deepcopy(config1)
        if "callbacks" in config2 and type(config2["callbacks"]) is dict:
            deprecation_warning(
                "callbacks dict interface",
                "a class extending rllib.algorithms.callbacks.DefaultCallbacks; "
                "see `rllib/examples/custom_metrics_and_callbacks.py` for an example.",
                error=True,
            )

        if _allow_unknown_configs is None:
            _allow_unknown_configs = cls._allow_unknown_configs
        return deep_update(
            config1,
            config2,
            _allow_unknown_configs,
            cls._allow_unknown_subkeys,
            cls._override_all_subkeys_if_type_changes,
            cls._override_all_key_list,
        )

    @staticmethod
    @ExperimentalAPI
    def validate_env(env: EnvType, env_context: EnvContext) -> None:
        """Env validator function for this Algorithm class.

        Override this in child classes to define custom validation
        behavior.

        Args:
            env: The (sub-)environment to validate. This is normally a
                single sub-environment (e.g. a gym.Env) within a vectorized
                setup.
            env_context: The EnvContext to configure the environment.

        Raises:
            Exception in case something is wrong with the given environment.
        """
        pass

    @override(Trainable)
    def _export_model(
        self, export_formats: List[str], export_dir: str
    ) -> Dict[str, str]:
        ExportFormat.validate(export_formats)
        exported = {}
        if ExportFormat.CHECKPOINT in export_formats:
            path = os.path.join(export_dir, ExportFormat.CHECKPOINT)
            self.export_policy_checkpoint(path)
            exported[ExportFormat.CHECKPOINT] = path
        if ExportFormat.MODEL in export_formats:
            path = os.path.join(export_dir, ExportFormat.MODEL)
            self.export_policy_model(path)
            exported[ExportFormat.MODEL] = path
        if ExportFormat.ONNX in export_formats:
            path = os.path.join(export_dir, ExportFormat.ONNX)
            self.export_policy_model(path, onnx=int(os.getenv("ONNX_OPSET", "11")))
            exported[ExportFormat.ONNX] = path
        return exported

    def import_model(self, import_file: str):
        """Imports a model from import_file.

        Note: Currently, only h5 files are supported.

        Args:
            import_file: The file to import the model from.

        Returns:
            A dict that maps ExportFormats to successfully exported models.
        """
        # Check for existence.
        if not os.path.exists(import_file):
            raise FileNotFoundError(
                "`import_file` '{}' does not exist! Can't import Model.".format(
                    import_file
                )
            )
        # Get the format of the given file.
        import_format = "h5"  # TODO(sven): Support checkpoint loading.

        ExportFormat.validate([import_format])
        if import_format != ExportFormat.H5:
            raise NotImplementedError
        else:
            return self.import_policy_model_from_h5(import_file)

    @PublicAPI
    def __getstate__(self) -> Dict:
        """Returns current state of Algorithm, sufficient to restore it from scratch.

        Returns:
            The current state dict of this Algorithm, which can be used to sufficiently
            restore the algorithm from scratch without any other information.
        """
        # Add config to state so complete Algorithm can be reproduced w/o it.
        state = {
            "algorithm_class": type(self),
            "config": self.config,
        }

        if hasattr(self, "workers"):
            state["worker"] = self.workers.local_worker().get_state()

        # Also store eval `policy_mapping_fn` (in case it's different from main one).
        # Note, the new `EnvRunner API` has no policy mapping function.
        if (
            hasattr(self, "evaluation_workers")
            and self.evaluation_workers is not None
            and not self.config.enable_env_runner_and_connector_v2
        ):
            state[
                "eval_policy_mapping_fn"
            ] = self.evaluation_workers.local_worker().policy_mapping_fn

        # TODO: Experimental functionality: Store contents of replay buffer
        #  to checkpoint, only if user has configured this.
        if self.local_replay_buffer is not None and self.config.get(
            "store_buffer_in_checkpoints"
        ):
            state["local_replay_buffer"] = self.local_replay_buffer.get_state()

        # New API stack: Save entire MetricsLogger state.
        if self.config.enable_env_runner_and_connector_v2:
            state["metrics_logger"] = self.metrics.get_state()
        # Old API stack: Save only counters.
        else:
            state["counters"] = self._counters

        # Save current `training_iteration`.
        state["training_iteration"] = self.training_iteration

        return state

    @PublicAPI
    def __setstate__(self, state) -> None:
        """Sets the algorithm to the provided state.

        Args:
            state: The state dict to restore this Algorithm instance to. `state` may
                have been returned by a call to an Algorithm's `__getstate__()` method.
        """
        # TODO (sven): Validate that our config and the config in state are compatible.
        #  For example, the model architectures may differ.
        #  Also, what should the behavior be if e.g. some training parameter
        #  (e.g. lr) changed?

        if hasattr(self, "workers") and "worker" in state and state["worker"]:
            self.workers.local_worker().set_state(state["worker"])
            remote_state_ref = ray.put(state["worker"])
            self.workers.foreach_worker(
                lambda w: w.set_state(ray.get(remote_state_ref)),
                local_worker=False,
            )
            if self.evaluation_workers:
                # Avoid `state` being pickled into the remote function below.
                _eval_policy_mapping_fn = state.get("eval_policy_mapping_fn")

                def _setup_eval_worker(w):
                    w.set_state(ray.get(remote_state_ref))
                    # Override `policy_mapping_fn` as it might be different for eval
                    # workers.
                    w.set_policy_mapping_fn(_eval_policy_mapping_fn)

                # If evaluation workers are used, also restore the policies
                # there in case they are used for evaluation purpose.
                self.evaluation_workers.foreach_worker(_setup_eval_worker)

        # If necessary, restore replay data as well.
        if self.local_replay_buffer is not None:
            # TODO: Experimental functionality: Restore contents of replay
            #  buffer from checkpoint, only if user has configured this.
            if self.config.get("store_buffer_in_checkpoints"):
                if "local_replay_buffer" in state:
                    self.local_replay_buffer.set_state(state["local_replay_buffer"])
                else:
                    logger.warning(
                        "`store_buffer_in_checkpoints` is True, but no replay "
                        "data found in state!"
                    )
            elif "local_replay_buffer" in state and log_once(
                "no_store_buffer_in_checkpoints_but_data_found"
            ):
                logger.warning(
                    "`store_buffer_in_checkpoints` is False, but some replay "
                    "data found in state!"
                )

        if self.config.enable_rl_module_and_learner:
            if "learner_state_dir" in state:
                self.learner_group.load_state(state["learner_state_dir"])
            else:
                logger.warning(
                    "You configured `enable_rl_module_and_learner=True`, but no "
                    "`learner_state_dir` key could be found in the state dict!"
                )
            # Recover MetricsLogger state.
            if "metrics_logger" in state:
                self.metrics.set_state(state["metrics_logger"])

        if "counters" in state:
            self._counters = state["counters"]

        if "training_iteration" in state:
            self._iteration = state["training_iteration"]

    @staticmethod
    def _checkpoint_info_to_algorithm_state(
        checkpoint_info: dict,
        policy_ids: Optional[Container[PolicyID]] = None,
        policy_mapping_fn: Optional[Callable[[AgentID, EpisodeID], PolicyID]] = None,
        policies_to_train: Optional[
            Union[
                Container[PolicyID],
                Callable[[PolicyID, Optional[SampleBatchType]], bool],
            ]
        ] = None,
    ) -> Dict:
        """Converts a checkpoint info or object to a proper Algorithm state dict.

        The returned state dict can be used inside self.__setstate__().

        Args:
            checkpoint_info: A checkpoint info dict as returned by
                `ray.rllib.utils.checkpoints.get_checkpoint_info(
                [checkpoint dir or AIR Checkpoint])`.
            policy_ids: Optional list/set of PolicyIDs. If not None, only those policies
                listed here will be included in the returned state. Note that
                state items such as filters, the `is_policy_to_train` function, as
                well as the multi-agent `policy_ids` dict will be adjusted as well,
                based on this arg.
            policy_mapping_fn: An optional (updated) policy mapping function
                to include in the returned state.
            policies_to_train: An optional list of policy IDs to be trained
                or a callable taking PolicyID and SampleBatchType and
                returning a bool (trainable or not?) to include in the returned state.

        Returns:
             The state dict usable within the `self.__setstate__()` method.
        """
        if checkpoint_info["type"] != "Algorithm":
            raise ValueError(
                "`checkpoint` arg passed to "
                "`Algorithm._checkpoint_info_to_algorithm_state()` must be an "
                f"Algorithm checkpoint (but is {checkpoint_info['type']})!"
            )

        msgpack = None
        if checkpoint_info.get("format") == "msgpack":
            msgpack = try_import_msgpack(error=True)

        with open(checkpoint_info["state_file"], "rb") as f:
            if msgpack is not None:
                state = msgpack.load(f)
            else:
                state = pickle.load(f)

        # New checkpoint format: Policies are in separate sub-dirs.
        # Note: Algorithms like ES/ARS don't have an EnvRunnerGroup, so we just return
        # the plain state here.
        if (
            checkpoint_info["checkpoint_version"] > version.Version("0.1")
            and state.get("worker") is not None
            and state.get("worker")
        ):
            worker_state = state["worker"]

            # Retrieve the set of all required policy IDs.
            policy_ids = set(
                policy_ids if policy_ids is not None else worker_state["policy_ids"]
            )

            # Remove those policies entirely from filters that are not in
            # `policy_ids`.
            worker_state["filters"] = {
                pid: filter
                for pid, filter in worker_state["filters"].items()
                if pid in policy_ids
            }

            # Get Algorithm class.
            if isinstance(state["algorithm_class"], str):
                # Try deserializing from a full classpath.
                # Or as a last resort: Tune registered algorithm name.
                state["algorithm_class"] = deserialize_type(
                    state["algorithm_class"]
                ) or get_trainable_cls(state["algorithm_class"])
            # Compile actual config object.
            default_config = state["algorithm_class"].get_default_config()
            if isinstance(default_config, AlgorithmConfig):
                new_config = default_config.update_from_dict(state["config"])
            else:
                new_config = Algorithm.merge_algorithm_configs(
                    default_config, state["config"]
                )

            # Remove policies from multiagent dict that are not in `policy_ids`.
            new_policies = new_config.policies
            if isinstance(new_policies, (set, list, tuple)):
                new_policies = {pid for pid in new_policies if pid in policy_ids}
            else:
                new_policies = {
                    pid: spec for pid, spec in new_policies.items() if pid in policy_ids
                }
            new_config.multi_agent(
                policies=new_policies,
                policies_to_train=policies_to_train,
                **(
                    {"policy_mapping_fn": policy_mapping_fn}
                    if policy_mapping_fn is not None
                    else {}
                ),
            )
            state["config"] = new_config

            # Prepare local `worker` state to add policies' states into it,
            # read from separate policy checkpoint files.
            worker_state["policy_states"] = {}
            for pid in policy_ids:
                policy_state_file = os.path.join(
                    checkpoint_info["checkpoint_dir"],
                    "policies",
                    pid,
                    "policy_state."
                    + ("msgpck" if checkpoint_info["format"] == "msgpack" else "pkl"),
                )
                if not os.path.isfile(policy_state_file):
                    raise ValueError(
                        "Given checkpoint does not seem to be valid! No policy "
                        f"state file found for PID={pid}. "
                        f"The file not found is: {policy_state_file}."
                    )

                with open(policy_state_file, "rb") as f:
                    if msgpack is not None:
                        worker_state["policy_states"][pid] = msgpack.load(f)
                    else:
                        worker_state["policy_states"][pid] = pickle.load(f)

            # These two functions are never serialized in a msgpack checkpoint (which
            # does not store code, unlike a cloudpickle checkpoint). Hence the user has
            # to provide them with the `Algorithm.from_checkpoint()` call.
            if policy_mapping_fn is not None:
                worker_state["policy_mapping_fn"] = policy_mapping_fn
            if (
                policies_to_train is not None
                # `policies_to_train` might be left None in case all policies should be
                # trained.
                or worker_state["is_policy_to_train"] == NOT_SERIALIZABLE
            ):
                worker_state["is_policy_to_train"] = policies_to_train

        if state["config"].enable_rl_module_and_learner:
            state["learner_state_dir"] = os.path.join(
                checkpoint_info["checkpoint_dir"], "learner"
            )

        return state

    @DeveloperAPI
    def _create_local_replay_buffer_if_necessary(
        self, config: PartialAlgorithmConfigDict
    ) -> Optional[MultiAgentReplayBuffer]:
        """Create a MultiAgentReplayBuffer instance if necessary.

        Args:
            config: Algorithm-specific configuration data.

        Returns:
            MultiAgentReplayBuffer instance based on algorithm config.
            None, if local replay buffer is not needed.
        """
        if not config.get("replay_buffer_config") or config["replay_buffer_config"].get(
            "no_local_replay_buffer"
        ):
            return

        return from_config(ReplayBuffer, config["replay_buffer_config"])

    def _run_one_training_iteration(self) -> Tuple[ResultDict, "TrainIterCtx"]:
        """Runs one training iteration (`self.iteration` will be +1 after this).

        Calls `self.training_step()` repeatedly until the configured minimum time (sec),
        minimum sample- or minimum training steps have been reached.

        Returns:
            The ResultDict from the last call to `training_step()`. Note that even
            though we only return the last ResultDict, the user stil has full control
            over the history and reduce behavior of individual metrics at the time these
            metrics are logged with `self.metrics.log_...()`.
        """
        with self._timers[TRAINING_ITERATION_TIMER]:
            # In case we are training (in a thread) parallel to evaluation,
            # we may have to re-enable eager mode here (gets disabled in the
            # thread).
            if self.config.get("framework") == "tf2" and not tf.executing_eagerly():
                tf1.enable_eager_execution()

            results = None
            # Create a step context ...
            with TrainIterCtx(algo=self) as train_iter_ctx:
                # .. so we can query it whether we should stop the iteration loop (e.g.
                # when we have reached `min_time_s_per_iteration`).
                while not train_iter_ctx.should_stop(results):
                    # Before training step, try to bring failed workers back.
                    with self._timers[RESTORE_WORKERS_TIMER]:
                        self.restore_workers(self.workers)

                    # Try to train one step.
                    with self._timers[TRAINING_STEP_TIMER]:
                        results = self.training_step()

        return results, train_iter_ctx

    def _run_one_evaluation(
        self,
        parallel_train_future: Optional[concurrent.futures.ThreadPoolExecutor] = None,
    ) -> ResultDict:
        """Runs evaluation step via `self.evaluate()` and handling worker failures.

        Args:
            parallel_train_future: In case, we are training and avaluating in parallel,
                this arg carries the currently running ThreadPoolExecutor object that
                runs the training iteration. Use `parallel_train_future.done()` to
                check, whether the parallel training job has completed and
                `parallel_train_future.result()` to get its return values.

        Returns:
            The results dict from the evaluation call.
        """

        if self.evaluation_workers is not None:
            with self._timers[RESTORE_EVAL_WORKERS_TIMER]:
                self.restore_workers(self.evaluation_workers)

        # Run `self.evaluate()` only once per training iteration.
        # TODO (sven): Move this timer into new metrics-logger API.
        with self._timers[EVALUATION_ITERATION_TIMER]:
            eval_results = self.evaluate(parallel_train_future=parallel_train_future)
        self._timers[EVALUATION_ITERATION_TIMER].push_units_processed(
            self._counters[NUM_ENV_STEPS_SAMPLED_FOR_EVALUATION_THIS_ITER]
        )

        # After evaluation, do a round of health check on remote eval workers to see if
        # any of the failed workers are back.
        if self.evaluation_workers is not None:
            # Add number of healthy evaluation workers after this iteration.
            eval_results[
                "num_healthy_workers"
            ] = self.evaluation_workers.num_healthy_remote_workers()
            eval_results[
                "num_in_flight_async_reqs"
            ] = self.evaluation_workers.num_in_flight_async_reqs()
            eval_results[
                "num_remote_worker_restarts"
            ] = self.evaluation_workers.num_remote_worker_restarts()

        # Evaluation does not run for every step.
        # Save evaluation metrics on Algorithm, so it can be attached to
        # subsequent step results as latest evaluation result.
        self.evaluation_metrics = {EVALUATION_RESULTS: eval_results}
        # To make the old stack forward compatible with the new API stack metrics
        # structure, we add everything under the new key (EVALUATION_RESULTS) as well as
        # the old one ("evaluation").
        if not self.config.enable_env_runner_and_connector_v2:
            self.evaluation_metrics["evaluation"] = eval_results

        return self.evaluation_metrics

    def _run_one_training_iteration_and_evaluation_in_parallel(
        self,
    ) -> Tuple[ResultDict, ResultDict, "TrainIterCtx"]:
        """Runs one training iteration and one evaluation step in parallel.

        First starts the training iteration (via `self._run_one_training_iteration()`)
        within a ThreadPoolExecutor, then runs the evaluation step in parallel.
        In auto-duration mode (config.evaluation_duration=auto), makes sure the
        evaluation step takes roughly the same time as the training iteration.

        Returns:
            A tuple containing the training results, the evaluation results, and
            the `TrainIterCtx` object returned by the training call.
        """
        with concurrent.futures.ThreadPoolExecutor() as executor:
            parallel_train_future = executor.submit(
                lambda: self._run_one_training_iteration()
            )
            evaluation_results = {}
            # If the debug setting _run_training_always_in_thread is used, do NOT
            # evaluate, no matter what the settings are,
            if not self.config._run_training_always_in_thread:
                # Pass the train_future into `self._run_one_evaluation()` to allow it
                # to run exactly as long as the training iteration takes in case
                # evaluation_duration=auto.
                evaluation_results = self._run_one_evaluation(
                    parallel_train_future=parallel_train_future
                )
            # Collect the training results from the future.
            train_results, train_iter_ctx = parallel_train_future.result()

        return train_results, evaluation_results, train_iter_ctx

    # Experimental method (trying to achieve evaluation and training in parallel w/o
    # using a thread pool).
    def _run_one_training_iteration_and_evaluation_in_parallel_wo_thread(
        self,
    ) -> Tuple[ResultDict, ResultDict, "TrainIterCtx"]:

        assert self.config.evaluation_duration != "auto"
        assert self.config.evaluation_duration_unit == "timesteps"
        assert self.evaluation_workers is not None

        with self._timers[RESTORE_EVAL_WORKERS_TIMER]:
            self.restore_workers(self.evaluation_workers)

        # Call the `_before_evaluate` hook.
        self._before_evaluate()

        # Sync weights to the evaluation EnvRunners.
        if self.evaluation_workers is not None:
            self.evaluation_workers.sync_weights(
                from_worker_or_learner_group=self.workers.local_worker()
            )
            self._sync_filters_if_needed(
                central_worker=self.workers.local_worker(),
                workers=self.evaluation_workers,
                config=self.evaluation_config,
            )

        self.callbacks.on_evaluate_start(algorithm=self, metrics_logger=self.metrics)

        env_steps = agent_steps = 0

        # Kick off sampling on all evaluation workers (async).
        # How many timesteps do we need to run?
        eval_cfg = self.evaluation_config
        time_out = self.config.evaluation_sample_timeout_s

        all_metrics = []
        algo_iteration = self.iteration

        # In case all the remote evaluation workers die during a round of
        # evaluation, we need to stop.
        units_per_healthy_remote_worker = (
            eval_cfg.rollout_fragment_length * eval_cfg.num_envs_per_env_runner
        )
        # Select proper number of evaluation workers for this round.
        selected_eval_worker_ids = [
            worker_id
            for i, worker_id in enumerate(self.evaluation_workers.healthy_worker_ids())
            if i * units_per_healthy_remote_worker < self.config.evaluation_duration
        ]
        self.evaluation_workers.foreach_worker_async(
            func=lambda w: (w.sample(), w.get_metrics(), algo_iteration),
            remote_worker_ids=selected_eval_worker_ids,
        )
        # Run training and collect the training results.
        train_results, train_iter_ctx = self._run_one_training_iteration()

        # Collect the evaluation results.
        eval_results = self.evaluation_workers.fetch_ready_async_reqs(
            return_obj_refs=False, timeout_seconds=time_out
        )
        for wid, (batch, metrics, iter) in eval_results:
            # Skip results from an older iteration.
            if iter != self.iteration:
                continue
            agent_steps += batch.agent_steps()
            env_steps += batch.env_steps()
            all_metrics.append(metrics)

        if not self.config.enable_env_runner_and_connector_v2:
            eval_results = summarize_episodes(
                all_metrics,
                all_metrics,
                keep_custom_metrics=(
                    self.evaluation_config.keep_per_episode_custom_metrics
                ),
            )
            # TODO: Don't dump sampler results into top-level.
            eval_results = dict({"sampler_results": eval_results}, **eval_results)
            eval_results[NUM_AGENT_STEPS_SAMPLED_THIS_ITER] = agent_steps
            eval_results[NUM_ENV_STEPS_SAMPLED_THIS_ITER] = env_steps
            # TODO: Remove this key at some point. Here for backward compatibility.
            eval_results["timesteps_this_iter"] = eval_results.get(
                NUM_ENV_STEPS_SAMPLED_THIS_ITER, 0
            )
        else:
            self.metrics.log_n_dicts(
                all_metrics,
                key=(EVALUATION_RESULTS, ENV_RUNNER_RESULTS),
            )
            eval_results = self.metrics.reduce((EVALUATION_RESULTS, ENV_RUNNER_RESULTS))

        # Warn if results are empty, it could be that this is because the eval timesteps
        # are not enough to run through one full episode.
        if eval_results["sampler_results"][NUM_EPISODES] == 0:
            logger.warning(
                "This evaluation iteration resulted in an empty set of episode summary "
                "results! It's possible that your configured duration timesteps are not"
                " enough to finish even a single episode. Your have configured "
                f"{self.config.evaluation_duration}"
                f"{self.config.evaluation_duration_unit}. For 'timesteps', try "
                "increasing this value via the `config.evaluation(evaluation_duration="
                "...)` OR change the unit to 'episodes' via `config.evaluation("
                "evaluation_duration_unit='episodes')` OR try increasing the timeout "
                "threshold via `config.evaluation(evaluation_sample_timeout_s=...)` OR "
                "you can also set `config.evaluation_force_reset_envs_before_iteration`"
                " to False. However, keep in mind that in the latter case, the "
                "evaluation results may contain some episode stats generated with "
                "earlier weights versions."
            )

        # After evaluation, do a round of health check on remote eval workers to see if
        # any of the failed workers are back.
        # Add number of healthy evaluation workers after this iteration.
        eval_results[
            "num_healthy_workers"
        ] = self.evaluation_workers.num_healthy_remote_workers()
        eval_results[
            "num_in_flight_async_reqs"
        ] = self.evaluation_workers.num_in_flight_async_reqs()
        eval_results[
            "num_remote_worker_restarts"
        ] = self.evaluation_workers.num_remote_worker_restarts()

        return train_results, {"evaluation": eval_results}, train_iter_ctx

    def _run_offline_evaluation(self):
        """Runs offline evaluation via `OfflineEvaluator.estimate_on_dataset()` API.

        This method will be used when `evaluation_dataset` is provided.
        Note: This will only work if the policy is a single agent policy.

        Returns:
            The results dict from the offline evaluation call.
        """
        assert len(self.workers.local_worker().policy_map) == 1

        parallelism = self.evaluation_config.evaluation_num_env_runners or 1
        offline_eval_results = {"off_policy_estimator": {}}
        for evaluator_name, offline_evaluator in self.reward_estimators.items():
            offline_eval_results["off_policy_estimator"][
                evaluator_name
            ] = offline_evaluator.estimate_on_dataset(
                self.evaluation_dataset,
                n_parallelism=parallelism,
            )
        return offline_eval_results

    @classmethod
    def _should_create_evaluation_rollout_workers(cls, eval_config: "AlgorithmConfig"):
        """Determines whether we need to create evaluation workers.

        Returns False if we need to run offline evaluation
        (with ope.estimate_on_dastaset API) or when local worker is to be used for
        evaluation. Note: We only use estimate_on_dataset API with bandits for now.
        That is when ope_split_batch_by_episode is False.
        TODO: In future we will do the same for episodic RL OPE.
        """
        run_offline_evaluation = (
            eval_config.off_policy_estimation_methods
            and not eval_config.ope_split_batch_by_episode
        )
        return not run_offline_evaluation and (
            eval_config.evaluation_num_env_runners > 0
            or eval_config.evaluation_interval
        )

    def _compile_iteration_results_new_api_stack(
        self, *, train_results, eval_results, step_ctx
    ):
        # Return dict (shallow copy of `train_results`).
        results: ResultDict = train_results.copy()
        # Evaluation results.
        if eval_results:
            results.update(eval_results)
        # Fault tolerance stats.
        results[FAULT_TOLERANCE_STATS] = {
            "num_healthy_workers": self.workers.num_healthy_remote_workers(),
            "num_in_flight_async_reqs": self.workers.num_in_flight_async_reqs(),
            "num_remote_worker_restarts": self.workers.num_remote_worker_restarts(),
        }
        # Resolve all `Stats` leafs by peeking (get their reduced values).
        return tree.map_structure(
            lambda s: s.peek() if isinstance(s, Stats) else s,
            results,
        )

    @OldAPIStack
    def _compile_iteration_results_old_and_hybrid_api_stacks(
        self, *, episodes_this_iter, step_ctx, iteration_results
    ):
        # Results to be returned.
        results: ResultDict = {}

        # Evaluation results.
        if "evaluation" in iteration_results:
            eval_results = iteration_results.pop("evaluation")
            iteration_results.pop(EVALUATION_RESULTS, None)
            results["evaluation"] = results[EVALUATION_RESULTS] = eval_results

        # Custom metrics and episode media.
        results["custom_metrics"] = iteration_results.pop("custom_metrics", {})
        results["episode_media"] = iteration_results.pop("episode_media", {})

        # Learner info.
        results["info"] = {LEARNER_INFO: iteration_results}

        # Calculate how many (if any) of older, historical episodes we have to add to
        # `episodes_this_iter` in order to reach the required smoothing window.
        episodes_for_metrics = episodes_this_iter[:]
        missing = self.config.metrics_num_episodes_for_smoothing - len(
            episodes_this_iter
        )
        # We have to add some older episodes to reach the smoothing window size.
        if missing > 0:
            episodes_for_metrics = self._episode_history[-missing:] + episodes_this_iter
            assert (
                len(episodes_for_metrics)
                <= self.config.metrics_num_episodes_for_smoothing
            )
        # Note that when there are more than `metrics_num_episodes_for_smoothing`
        # episodes in `episodes_for_metrics`, leave them as-is. In this case, we'll
        # compute the stats over that larger number.

        # Add new episodes to our history and make sure it doesn't grow larger than
        # needed.
        self._episode_history.extend(episodes_this_iter)
        self._episode_history = self._episode_history[
            -self.config.metrics_num_episodes_for_smoothing :
        ]
        results["sampler_results"] = results[ENV_RUNNER_RESULTS] = summarize_episodes(
            episodes_for_metrics,
            episodes_this_iter,
            self.config.keep_per_episode_custom_metrics,
        )
        # TODO: Don't dump sampler results into top-level.
        results.update(results["sampler_results"])

        results["num_healthy_workers"] = self.workers.num_healthy_remote_workers()
        results["num_in_flight_async_reqs"] = self.workers.num_in_flight_async_reqs()
        results[
            "num_remote_worker_restarts"
        ] = self.workers.num_remote_worker_restarts()

        # Train-steps- and env/agent-steps this iteration.
        for c in [
            NUM_AGENT_STEPS_SAMPLED,
            NUM_AGENT_STEPS_TRAINED,
            NUM_ENV_STEPS_SAMPLED,
            NUM_ENV_STEPS_TRAINED,
        ]:
            results[c] = self._counters[c]
        time_taken_sec = step_ctx.get_time_taken_sec()
        if self.config.count_steps_by == "agent_steps":
            results[NUM_AGENT_STEPS_SAMPLED + "_this_iter"] = step_ctx.sampled
            results[NUM_AGENT_STEPS_TRAINED + "_this_iter"] = step_ctx.trained
            results[NUM_AGENT_STEPS_SAMPLED + "_throughput_per_sec"] = (
                step_ctx.sampled / time_taken_sec
            )
            results[NUM_AGENT_STEPS_TRAINED + "_throughput_per_sec"] = (
                step_ctx.trained / time_taken_sec
            )
            # TODO: For CQL and other algos, count by trained steps.
            results["timesteps_total"] = self._counters[NUM_AGENT_STEPS_SAMPLED]
        else:
            results[NUM_ENV_STEPS_SAMPLED + "_this_iter"] = step_ctx.sampled
            results[NUM_ENV_STEPS_TRAINED + "_this_iter"] = step_ctx.trained
            results[NUM_ENV_STEPS_SAMPLED + "_throughput_per_sec"] = (
                step_ctx.sampled / time_taken_sec
            )
            results[NUM_ENV_STEPS_TRAINED + "_throughput_per_sec"] = (
                step_ctx.trained / time_taken_sec
            )
            # TODO: For CQL and other algos, count by trained steps.
            results["timesteps_total"] = self._counters[NUM_ENV_STEPS_SAMPLED]

        # Forward compatibility with new API stack.
        results[NUM_ENV_STEPS_SAMPLED_LIFETIME] = results["timesteps_total"]
        results[NUM_AGENT_STEPS_SAMPLED_LIFETIME] = self._counters[
            NUM_AGENT_STEPS_SAMPLED
        ]

        # TODO: Backward compatibility.
        results[STEPS_TRAINED_THIS_ITER_COUNTER] = step_ctx.trained
        results["agent_timesteps_total"] = self._counters[NUM_AGENT_STEPS_SAMPLED]

        # Process timer results.
        timers = {}
        for k, timer in self._timers.items():
            timers["{}_time_ms".format(k)] = round(timer.mean * 1000, 3)
            if timer.has_units_processed():
                timers["{}_throughput".format(k)] = round(timer.mean_throughput, 3)
        results["timers"] = timers

        # Process counter results.
        counters = {}
        for k, counter in self._counters.items():
            counters[k] = counter
        results["counters"] = counters
        # TODO: Backward compatibility.
        results["info"].update(counters)

        return results

    def __repr__(self):
        return type(self).__name__

    def _record_usage(self, config):
        """Record the framework and algorithm used.

        Args:
            config: Algorithm config dict.
        """
        record_extra_usage_tag(TagKey.RLLIB_FRAMEWORK, config["framework"])
        record_extra_usage_tag(TagKey.RLLIB_NUM_WORKERS, str(config["num_workers"]))
        alg = self.__class__.__name__
        # We do not want to collect user defined algorithm names.
        if alg not in ALL_ALGORITHMS:
            alg = "USER_DEFINED"
        record_extra_usage_tag(TagKey.RLLIB_ALGORITHM, alg)

    @Deprecated(error=False)
    def import_policy_model_from_h5(
        self,
        import_file: str,
        policy_id: PolicyID = DEFAULT_POLICY_ID,
    ) -> None:
        """Imports a policy's model with given policy_id from a local h5 file.

        Args:
            import_file: The h5 file to import from.
            policy_id: Optional policy id to import into.

        """
        self.get_policy(policy_id).import_model_from_h5(import_file)
        # Sync new weights to remote workers.
        self.workers.sync_weights()


class TrainIterCtx:
    def __init__(self, algo: Algorithm):
        self.algo = algo
        self.time_start = None
        self.time_stop = None

    def __enter__(self):
        # Before first call to `step()`, `results` is expected to be None ->
        # Start with self.failures=-1 -> set to 0 before the very first call
        # to `self.step()`.
        self.failures = -1

        self.time_start = time.time()
        self.sampled = 0
        self.trained = 0
        if self.algo.config.enable_env_runner_and_connector_v2:
            self.init_env_steps_sampled = self.algo.metrics.peek(
                NUM_ENV_STEPS_SAMPLED_LIFETIME
            )
            self.init_env_steps_trained = self.algo.metrics.peek(
                NUM_ENV_STEPS_TRAINED_LIFETIME
            )
            self.init_agent_steps_sampled = sum(
                self.algo.metrics.peek(NUM_AGENT_STEPS_SAMPLED_LIFETIME).values()
            )
            self.init_agent_steps_trained = sum(
                self.algo.metrics.peek(NUM_AGENT_STEPS_TRAINED_LIFETIME).values()
            )
        else:
            self.init_env_steps_sampled = self.algo._counters[NUM_ENV_STEPS_SAMPLED]
            self.init_env_steps_trained = self.algo._counters[NUM_ENV_STEPS_TRAINED]
            self.init_agent_steps_sampled = self.algo._counters[NUM_AGENT_STEPS_SAMPLED]
            self.init_agent_steps_trained = self.algo._counters[NUM_AGENT_STEPS_TRAINED]
        self.failure_tolerance = (
            self.algo.config.num_consecutive_env_runner_failures_tolerance
        )
        return self

    def __exit__(self, *args):
        self.time_stop = time.time()

    def get_time_taken_sec(self) -> float:
        """Returns the time we spent in the context in seconds."""
        return self.time_stop - self.time_start

    def should_stop(self, results):
        # Before first call to `step()`.
        if results is None:
            # Fail after n retries.
            self.failures += 1
            if self.failures > self.failure_tolerance:
                raise RuntimeError(
                    "More than `num_consecutive_env_runner_failures_tolerance="
                    f"{self.failure_tolerance}` consecutive worker failures! "
                    "Exiting."
                )
            # Continue to very first `step()` call or retry `step()` after
            # a (tolerable) failure.
            return False

        # Stopping criteria.
        if self.algo.config.enable_env_runner_and_connector_v2:
            if self.algo.config.count_steps_by == "agent_steps":
                self.sampled = (
                    sum(
                        self.algo.metrics.peek(
                            NUM_AGENT_STEPS_SAMPLED_LIFETIME
                        ).values()
                    )
                    - self.init_agent_steps_sampled
                )
                self.trained = (
                    sum(
                        self.algo.metrics.peek(
                            NUM_AGENT_STEPS_TRAINED_LIFETIME
                        ).values()
                    )
                    - self.init_agent_steps_trained
                )
            else:
                self.sampled = (
                    self.algo.metrics.peek(NUM_ENV_STEPS_SAMPLED_LIFETIME)
                    - self.init_env_steps_sampled
                )
                self.trained = (
                    self.algo.metrics.peek(NUM_ENV_STEPS_TRAINED_LIFETIME)
                    - self.init_env_steps_trained
                )
        else:
            if self.algo.config.count_steps_by == "agent_steps":
                self.sampled = (
                    self.algo._counters[NUM_AGENT_STEPS_SAMPLED]
                    - self.init_agent_steps_sampled
                )
                self.trained = (
                    self.algo._counters[NUM_AGENT_STEPS_TRAINED]
                    - self.init_agent_steps_trained
                )
            else:
                self.sampled = (
                    self.algo._counters[NUM_ENV_STEPS_SAMPLED]
                    - self.init_env_steps_sampled
                )
                self.trained = (
                    self.algo._counters[NUM_ENV_STEPS_TRAINED]
                    - self.init_env_steps_trained
                )

        min_t = self.algo.config.min_time_s_per_iteration
        min_sample_ts = self.algo.config.min_sample_timesteps_per_iteration
        min_train_ts = self.algo.config.min_train_timesteps_per_iteration
        # Repeat if not enough time has passed or if not enough
        # env|train timesteps have been processed (or these min
        # values are not provided by the user).
        if (
            (not min_t or time.time() - self.time_start >= min_t)
            and (not min_sample_ts or self.sampled >= min_sample_ts)
            and (not min_train_ts or self.trained >= min_train_ts)
        ):
            return True
        else:
            return False

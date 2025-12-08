import concurrent
import copy
import functools
import importlib
import importlib.metadata
import json
import logging
import os
import pathlib
import re
import tempfile
import time
from collections import defaultdict
from datetime import datetime
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Collection,
    DefaultDict,
    Dict,
    List,
    Optional,
    Set,
    Tuple,
    Type,
    Union,
)

import gymnasium as gym
import numpy as np
import pyarrow.fs
import tree  # pip install dm_tree
from packaging import version

import ray
import ray.cloudpickle as pickle
from ray._common.deprecation import (
    DEPRECATED_VALUE,
    Deprecated,
    deprecation_warning,
)
from ray._common.usage.usage_lib import TagKey, record_extra_usage_tag
from ray.actor import ActorHandle
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.algorithms.registry import ALGORITHMS_CLASS_TO_NAME as ALL_ALGORITHMS
from ray.rllib.algorithms.utils import (
    AggregatorActor,
    _get_env_runner_bundles,
    _get_learner_bundles,
    _get_main_process_bundle,
    _get_offline_eval_runner_bundles,
)
from ray.rllib.callbacks.utils import make_callback
from ray.rllib.connectors.agent.obs_preproc import ObsPreprocessorConnector
from ray.rllib.connectors.connector_pipeline_v2 import ConnectorPipelineV2
from ray.rllib.core import (
    COMPONENT_ENV_RUNNER,
    COMPONENT_ENV_TO_MODULE_CONNECTOR,
    COMPONENT_EVAL_ENV_RUNNER,
    COMPONENT_LEARNER,
    COMPONENT_LEARNER_GROUP,
    COMPONENT_METRICS_LOGGER,
    COMPONENT_MODULE_TO_ENV_CONNECTOR,
    COMPONENT_RL_MODULE,
    DEFAULT_MODULE_ID,
)
from ray.rllib.core.columns import Columns
from ray.rllib.core.rl_module import validate_module_id
from ray.rllib.core.rl_module.multi_rl_module import (
    MultiRLModule,
    MultiRLModuleSpec,
)
from ray.rllib.core.rl_module.rl_module import RLModule, RLModuleSpec
from ray.rllib.env import INPUT_ENV_SPACES
from ray.rllib.env.env_context import EnvContext
from ray.rllib.env.env_runner import EnvRunner
from ray.rllib.env.env_runner_group import EnvRunnerGroup
from ray.rllib.env.utils import _gym_env_creator
from ray.rllib.evaluation.metrics import (
    collect_episodes,
    summarize_episodes,
)
from ray.rllib.execution.rollout_ops import synchronous_parallel_sample
from ray.rllib.offline import get_dataset_and_shards
from ray.rllib.offline.estimators import (
    DirectMethod,
    DoublyRobust,
    ImportanceSampling,
    OffPolicyEstimator,
    WeightedImportanceSampling,
)
from ray.rllib.offline.offline_evaluator import OfflineEvaluator
from ray.rllib.policy.policy import Policy, PolicySpec
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID, SampleBatch
from ray.rllib.utils import FilterManager, deep_update, force_list
from ray.rllib.utils.actor_manager import FaultTolerantActorManager
from ray.rllib.utils.annotations import (
    DeveloperAPI,
    ExperimentalAPI,
    OldAPIStack,
    OverrideToImplementCustomLogic,
    OverrideToImplementCustomLogic_CallToSuperRecommended,
    PublicAPI,
    override,
)
from ray.rllib.utils.checkpoints import (
    CHECKPOINT_VERSION,
    CHECKPOINT_VERSION_LEARNER_AND_ENV_RUNNER,
    Checkpointable,
    get_checkpoint_info,
    try_import_msgpack,
)
from ray.rllib.utils.debug import update_global_seed_if_necessary
from ray.rllib.utils.error import ERR_MSG_INVALID_ENV_DESCRIPTOR, EnvError
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.from_config import from_config
from ray.rllib.utils.metrics import (
    AGGREGATOR_ACTOR_RESULTS,
    ALL_MODULES,
    DATASET_NUM_ITERS_EVALUATED,
    ENV_RUNNER_RESULTS,
    ENV_RUNNER_SAMPLING_TIMER,
    EPISODE_LEN_MEAN,
    EPISODE_RETURN_MEAN,
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
    NUM_ENV_STEPS_SAMPLED_FOR_EVALUATION_THIS_ITER,
    NUM_ENV_STEPS_SAMPLED_LIFETIME,
    NUM_ENV_STEPS_SAMPLED_THIS_ITER,
    NUM_ENV_STEPS_TRAINED,
    NUM_ENV_STEPS_TRAINED_LIFETIME,
    NUM_EPISODES,
    NUM_EPISODES_LIFETIME,
    NUM_TRAINING_STEP_CALLS_PER_ITERATION,
    OFFLINE_EVAL_RUNNER_RESULTS,
    OFFLINE_EVALUATION_ITERATION_TIMER,
    RESTORE_ENV_RUNNERS_TIMER,
    RESTORE_EVAL_ENV_RUNNERS_TIMER,
    RESTORE_OFFLINE_EVAL_RUNNERS_TIMER,
    STEPS_TRAINED_THIS_ITER_COUNTER,
    SYNCH_ENV_CONNECTOR_STATES_TIMER,
    SYNCH_EVAL_ENV_CONNECTOR_STATES_TIMER,
    SYNCH_WORKER_WEIGHTS_TIMER,
    TIMERS,
    TRAINING_ITERATION_TIMER,
    TRAINING_STEP_TIMER,
)
from ray.rllib.utils.metrics.learner_info import LEARNER_INFO
from ray.rllib.utils.metrics.metrics_logger import MetricsLogger
from ray.rllib.utils.metrics.ray_metrics import (
    DEFAULT_HISTOGRAM_BOUNDARIES_LONG_EVENTS,
    DEFAULT_HISTOGRAM_BOUNDARIES_SHORT_EVENTS,
    TimerAndPrometheusLogger,
)
from ray.rllib.utils.replay_buffers import MultiAgentReplayBuffer, ReplayBuffer
from ray.rllib.utils.runners.runner_group import RunnerGroup
from ray.rllib.utils.serialization import NOT_SERIALIZABLE, deserialize_type
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
    StateDict,
    TensorStructType,
    TensorType,
)
from ray.train.constants import DEFAULT_STORAGE_PATH
from ray.tune import Checkpoint
from ray.tune.execution.placement_groups import PlacementGroupFactory
from ray.tune.experiment.trial import ExportFormat
from ray.tune.logger import Logger, UnifiedLogger
from ray.tune.registry import ENV_CREATOR, _global_registry, get_trainable_cls
from ray.tune.resources import Resources
from ray.tune.result import TRAINING_ITERATION
from ray.tune.trainable import Trainable
from ray.util import log_once
from ray.util.metrics import Counter, Histogram
from ray.util.timer import _Timer

if TYPE_CHECKING:
    from ray.rllib.core.learner.learner_group import LearnerGroup
    from ray.rllib.offline.offline_data import OfflineData

tf1, tf, tfv = try_import_tf()

logger = logging.getLogger(__name__)


@PublicAPI
class Algorithm(Checkpointable, Trainable):
    """An RLlib algorithm responsible for training one or more neural network models.

    You can write your own Algorithm classes by sub-classing from `Algorithm`
    or any of its built-in subclasses.
    Override the `training_step` method to implement your own algorithm logic.
    Find the various built-in `training_step()` methods for different algorithms in
    their respective [algo name].py files, for example:
    `ray.rllib.algorithms.dqn.dqn.py` or `ray.rllib.algorithms.impala.impala.py`.

    The most important API methods an Algorithm exposes are `train()` for running a
    single training iteration, `evaluate()` for running a single round of evaluation,
    `save_to_path()` for creating a checkpoint, and `restore_from_path()` for loading a
    state from an existing checkpoint.
    """

    #: The AlgorithmConfig instance of the Algorithm.
    config: Optional[AlgorithmConfig] = None
    #: The MetricsLogger instance of the Algorithm. RLlib uses this to log
    #: metrics from within the `training_step()` method. Users can use it to log
    #: metrics from within their custom Algorithm-based callbacks.
    metrics: Optional[MetricsLogger] = None
    #: The `EnvRunnerGroup` of the Algorithm. An `EnvRunnerGroup` is
    #: composed of a single local `EnvRunner` (see: `self.env_runner`), serving as
    #: the reference copy of the models to be trained and optionally one or more
    #: remote `EnvRunners` used to generate training samples from the RL
    #: environment, in parallel. EnvRunnerGroup is fault-tolerant and elastic. It
    #: tracks health states for all the managed remote EnvRunner actors. As a
    #: result, Algorithm should never access the underlying actor handles directly.
    #: Instead, always access them via all the foreach APIs with assigned IDs of
    #: the underlying EnvRunners.
    env_runner_group: Optional[EnvRunnerGroup] = None
    #: A special EnvRunnerGroup only used for evaluation, not to
    #: collect training samples.
    eval_env_runner_group: Optional[EnvRunnerGroup] = None
    #: The `LearnerGroup` instance of the Algorithm, managing either
    #: one local `Learner` or one or more remote `Learner` actors. Responsible for
    #: updating the models from RL environment (episode) data.
    learner_group: Optional["LearnerGroup"] = None
    #: An optional OfflineData instance, used for offline RL.
    offline_data: Optional["OfflineData"] = None

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
        "custom_resources_per_env_runner",
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
        f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}",
        f"{EVALUATION_RESULTS}/{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}",
        f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}",
        f"{NUM_ENV_STEPS_TRAINED_LIFETIME}",
        f"{NUM_EPISODES_LIFETIME}",
        f"{ENV_RUNNER_RESULTS}/{EPISODE_LEN_MEAN}",
    )

    # Backward compatibility with old checkpoint system (now through the
    # `Checkpointable` API).
    METADATA_FILE_NAME = "rllib_checkpoint.json"
    STATE_FILE_NAME = "algorithm_state"

    @classmethod
    @override(Checkpointable)
    def from_checkpoint(
        cls,
        path: Union[str, Checkpoint],
        filesystem: Optional["pyarrow.fs.FileSystem"] = None,
        *,
        # @OldAPIStack
        policy_ids: Optional[Collection[PolicyID]] = None,
        policy_mapping_fn: Optional[Callable[[AgentID, EpisodeID], PolicyID]] = None,
        policies_to_train: Optional[
            Union[
                Collection[PolicyID],
                Callable[[PolicyID, Optional[SampleBatchType]], bool],
            ]
        ] = None,
        # deprecated args
        checkpoint=DEPRECATED_VALUE,
        **kwargs,
    ) -> "Algorithm":
        """Creates a new algorithm instance from a given checkpoint.

        Args:
            path: The path (str) to the checkpoint directory to use or a Ray Train
                Checkpoint instance to restore from.
            filesystem: PyArrow FileSystem to use to access data at the `path`. If not
                specified, this is inferred from the URI scheme of `path`.
            policy_ids: Optional list of PolicyIDs to recover. This allows users to
                restore an Algorithm with only a subset of the originally present
                Policies.
            policy_mapping_fn: An optional (updated) policy mapping function to use from
                here on.
            policies_to_train: An optional list of policy IDs to be trained or a
                callable taking PolicyID and SampleBatchType and returning a bool
                (trainable or not?). If None, will keep the existing setup in place.
                Policies, whose IDs are not in the list (or for which the callable
                returns False) will not be updated.

        Returns:
            The instantiated Algorithm.
        """
        if checkpoint != DEPRECATED_VALUE:
            deprecation_warning(
                old="Algorithm.from_checkpoint(checkpoint=...)",
                new="Algorithm.from_checkpoint(path=...)",
                error=True,
            )
        checkpoint_info = get_checkpoint_info(path, filesystem)

        # New API stack -> Use Checkpointable's default implementation.
        if checkpoint_info["checkpoint_version"] >= version.Version("2.0"):
            # `path` is a Checkpoint instance: Translate to directory and continue.
            if isinstance(path, Checkpoint):
                path = path.to_directory()
            return super().from_checkpoint(path, filesystem=filesystem, **kwargs)

        # Not possible for (v0.1) (algo class and config information missing
        # or very hard to retrieve).
        elif checkpoint_info["checkpoint_version"] == version.Version("0.1"):
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
        # Translate possible dict into an AlgorithmConfig object, as well as,
        # resolving generic config objects into specific ones (e.g. passing
        # an `AlgorithmConfig` super-class instance into a PPO constructor,
        # which normally would expect a PPOConfig object).
        if isinstance(config, dict):
            default_config = self.get_default_config()
            # `self.get_default_config()` also returned a dict ->
            # Last resort: Create core AlgorithmConfig from merged dicts.
            if isinstance(default_config, dict):
                if "class" in config:
                    AlgorithmConfig.from_state(config)
                else:
                    config = AlgorithmConfig.from_dict(
                        config_dict=self.merge_algorithm_configs(
                            default_config, config, True
                        )
                    )

            # Default config is an AlgorithmConfig -> update its properties
            # from the given config dict.
            else:
                if isinstance(config, dict) and "class" in config:
                    config = default_config.from_state(config)
                else:
                    config = default_config.update_from_dict(config)
        else:
            default_config = self.get_default_config()
            # Given AlgorithmConfig is not of the same type as the default config:
            # This could be the case e.g. if the user is building an algo from a
            # generic AlgorithmConfig() object.
            if not isinstance(config, type(default_config)):
                config = default_config.update_from_dict(config.to_dict())
            else:
                config = default_config.from_state(config.get_state())

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
        self.metrics: MetricsLogger = MetricsLogger(
            root=True, stats_cls_lookup=config.stats_cls_lookup
        )

        # Create a default logger creator if no logger_creator is specified
        if logger_creator is None:
            # Default logdir prefix containing the agent's name and the
            # env id.
            timestr = datetime.today().strftime("%Y-%m-%d_%H-%M-%S")
            env_descr_for_dir = re.sub("[/\\\\]", "-", str(env_descr))
            logdir_prefix = f"{type(self).__name__}_{env_descr_for_dir}_{timestr}"

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
        self.eval_env_runner_group: Optional[EnvRunnerGroup] = None

        # Ray metrics - Algorithm
        self._metrics_step_time: Optional[Histogram] = None
        self._metrics_run_one_training_iteration_time: Optional[Histogram] = None
        self._metrics_run_one_evaluation_time: Optional[Histogram] = None
        self._metrics_compile_iteration_results_time: Optional[Histogram] = None
        self._metrics_training_step_time: Optional[Histogram] = None
        self._metrics_evaluate_time: Optional[Histogram] = None
        self._metrics_evaluate_sync_env_runner_weights_time: Optional[Histogram] = None
        self._metrics_evaluate_sync_connector_states_time: Optional[Histogram] = None
        self._metrics_step_sync_env_runner_states_time: Optional[Histogram] = None
        self._metrics_load_checkpoint_time: Optional[Histogram] = None
        self._metrics_save_checkpoint_time: Optional[Histogram] = None

        # Ray metrics - Algorithm callbacks
        self._metrics_callback_on_train_result_time: Optional[Histogram] = None
        self._metrics_callback_on_evaluate_start_time: Optional[Histogram] = None
        self._metrics_callback_on_evaluate_end_time: Optional[Histogram] = None

        # Ray metrics - IMPALA
        self._metrics_impala_training_step_time: Optional[Histogram] = None
        self._metrics_impala_training_step_aggregator_preprocessing_time: Optional[
            Histogram
        ] = None
        self._metrics_impala_training_step_learner_group_loop_time: Optional[
            Histogram
        ] = None
        self._metrics_impala_training_step_sync_env_runner_state_time: Optional[
            Histogram
        ] = None
        self._metrics_impala_sample_and_get_connector_states_time: Optional[
            Histogram
        ] = None
        self._metrics_impala_training_step_input_batches: Optional[Counter] = None
        self._metrics_impala_training_step_zero_input_batches: Optional[Counter] = None
        self._metrics_impala_training_step_env_steps_dropped: Optional[Counter] = None

        super().__init__(
            config=config,
            logger_creator=logger_creator,
            **kwargs,
        )

    def _set_up_metrics(self):
        self._metrics_step_time = Histogram(
            name="rllib_algorithm_step_time",
            description="Time spent in Algorithm.step()",
            boundaries=DEFAULT_HISTOGRAM_BOUNDARIES_LONG_EVENTS,
            tag_keys=("rllib",),
        )
        self._metrics_step_time.set_default_tags({"rllib": self.__class__.__name__})

        self._metrics_run_one_training_iteration_time = Histogram(
            name="rllib_algorithm_run_one_training_iteration_time",
            description="Time spent in Algorithm._run_one_training_iteration()",
            boundaries=DEFAULT_HISTOGRAM_BOUNDARIES_LONG_EVENTS,
            tag_keys=("rllib",),
        )
        self._metrics_run_one_training_iteration_time.set_default_tags(
            {"rllib": self.__class__.__name__}
        )

        self._metrics_run_one_evaluation_time = Histogram(
            name="rllib_algorithm_run_one_evaluation_time",
            description="Time spent in Algorithm._run_one_evaluation()",
            boundaries=DEFAULT_HISTOGRAM_BOUNDARIES_LONG_EVENTS,
            tag_keys=("rllib",),
        )
        self._metrics_run_one_evaluation_time.set_default_tags(
            {"rllib": self.__class__.__name__}
        )

        self._metrics_compile_iteration_results_time = Histogram(
            name="rllib_algorithm_compile_iteration_results_time",
            description="Time spent in Algorithm._compile_iteration_results()",
            boundaries=DEFAULT_HISTOGRAM_BOUNDARIES_SHORT_EVENTS,
            tag_keys=("rllib",),
        )
        self._metrics_compile_iteration_results_time.set_default_tags(
            {"rllib": self.__class__.__name__}
        )

        self._metrics_training_step_time = Histogram(
            name="rllib_algorithm_training_step_time",
            description="Time spent in Algorithm.training_step()",
            boundaries=DEFAULT_HISTOGRAM_BOUNDARIES_LONG_EVENTS,
            tag_keys=("rllib",),
        )
        self._metrics_training_step_time.set_default_tags(
            {"rllib": self.__class__.__name__}
        )

        self._metrics_evaluate_time = Histogram(
            name="rllib_algorithm_evaluate_time",
            description="Time spent in Algorithm.evaluate()",
            boundaries=DEFAULT_HISTOGRAM_BOUNDARIES_LONG_EVENTS,
            tag_keys=("rllib",),
        )
        self._metrics_evaluate_time.set_default_tags({"rllib": self.__class__.__name__})

        self._metrics_evaluate_sync_env_runner_weights_time = Histogram(
            name="rllib_algorithm_evaluate_sync_env_runner_weights_time",
            description="Time spent on syncing weights to the eval EnvRunners in the Algorithm.evaluate()",
            boundaries=DEFAULT_HISTOGRAM_BOUNDARIES_SHORT_EVENTS,
            tag_keys=("rllib",),
        )
        self._metrics_evaluate_sync_env_runner_weights_time.set_default_tags(
            {"rllib": self.__class__.__name__}
        )

        self._metrics_evaluate_sync_connector_states_time = Histogram(
            name="rllib_algorithm_evaluate_sync_connector_states_time",
            description="Time spent on syncing connector states in the Algorithm.evaluate()",
            boundaries=DEFAULT_HISTOGRAM_BOUNDARIES_SHORT_EVENTS,
            tag_keys=("rllib",),
        )
        self._metrics_evaluate_sync_connector_states_time.set_default_tags(
            {"rllib": self.__class__.__name__}
        )

        self._metrics_step_sync_env_runner_states_time = Histogram(
            name="rllib_algorithm_step_sync_env_runner_states_time",
            description="Time spent in sync_env_runner_states code block of the Algorithm.step()",
            boundaries=DEFAULT_HISTOGRAM_BOUNDARIES_SHORT_EVENTS,
            tag_keys=("rllib",),
        )
        self._metrics_step_sync_env_runner_states_time.set_default_tags(
            {"rllib": self.__class__.__name__}
        )

        self._metrics_load_checkpoint_time = Histogram(
            name="rllib_algorithm_load_checkpoint_time",
            description="Time spent in Algorithm.load_checkpoint()",
            boundaries=DEFAULT_HISTOGRAM_BOUNDARIES_SHORT_EVENTS,
            tag_keys=("rllib",),
        )
        self._metrics_load_checkpoint_time.set_default_tags(
            {"rllib": self.__class__.__name__}
        )

        self._metrics_save_checkpoint_time = Histogram(
            name="rllib_algorithm_save_checkpoint_time",
            description="Time spent in Algorithm.save_checkpoint()",
            boundaries=DEFAULT_HISTOGRAM_BOUNDARIES_SHORT_EVENTS,
            tag_keys=("rllib",),
        )
        self._metrics_save_checkpoint_time.set_default_tags(
            {"rllib": self.__class__.__name__}
        )

        # Ray metrics - Algorithm callbacks
        self._metrics_callback_on_train_result_time = Histogram(
            name="rllib_algorithm_callback_on_train_result_time",
            description="Time spent in callback 'on_train_result()'",
            boundaries=DEFAULT_HISTOGRAM_BOUNDARIES_SHORT_EVENTS,
            tag_keys=("rllib",),
        )
        self._metrics_callback_on_train_result_time.set_default_tags(
            {"rllib": self.__class__.__name__}
        )

        self._metrics_callback_on_evaluate_start_time = Histogram(
            name="rllib_algorithm_callback_on_evaluate_start_time",
            description="Time spent in callback 'on_evaluate_start()'",
            boundaries=DEFAULT_HISTOGRAM_BOUNDARIES_SHORT_EVENTS,
            tag_keys=("rllib",),
        )
        self._metrics_callback_on_evaluate_start_time.set_default_tags(
            {"rllib": self.__class__.__name__}
        )

        self._metrics_callback_on_evaluate_end_time = Histogram(
            name="rllib_algorithm_callback_on_evaluate_end_time",
            description="Time spent in callback 'on_evaluate_end()'",
            boundaries=DEFAULT_HISTOGRAM_BOUNDARIES_SHORT_EVENTS,
            tag_keys=("rllib",),
        )
        self._metrics_callback_on_evaluate_end_time.set_default_tags(
            {"rllib": self.__class__.__name__}
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
        return self.env_runner_group.healthy_worker_ids()

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
        if self.config.enable_env_runner_and_connector_v2:
            self.callbacks = [cls() for cls in force_list(self.config.callbacks_class)]
        else:
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

        self.env_runner_group: Optional[EnvRunnerGroup] = None
        # In case there is no local EnvRunner anymore, we need to handle connector
        # pipelines directly here.
        self.spaces: Optional[Dict] = None
        self.env_to_module_connector: Optional[ConnectorPipelineV2] = None
        self.module_to_env_connector: Optional[ConnectorPipelineV2] = None

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

        # If an input path is available and we are on the new API stack generate
        # an `OfflineData` instance.
        if self.config.is_offline:
            from ray.rllib.offline.offline_data import OfflineData

            # Use either user-provided `OfflineData` class or RLlib's default.
            offline_data_class = self.config.offline_data_class or OfflineData
            # Build the `OfflineData` class.
            self.offline_data = offline_data_class(self.config)
        # Otherwise set the attribute to `None`.
        else:
            self.offline_data = None

        if self.config.is_online or not self.config.enable_env_runner_and_connector_v2:
            # Create a set of env runner actors via a EnvRunnerGroup.
            self.env_runner_group = EnvRunnerGroup(
                env_creator=self.env_creator,
                validate_env=self.validate_env,
                default_policy_class=self.get_default_policy_class(self.config),
                config=self.config,
                # New API stack: User decides whether to create local env runner.
                # Old API stack: Always create local EnvRunner.
                local_env_runner=(
                    True
                    if not self.config.enable_env_runner_and_connector_v2
                    else self.config.create_local_env_runner
                ),
                logdir=self.logdir,
                tune_trial_id=self.trial_id,
            )

        # Compile, validate, and freeze an evaluation config.
        self.evaluation_config = self.config.get_evaluation_config_object()
        self.evaluation_config.validate()
        self.evaluation_config.freeze()

        # Evaluation EnvRunnerGroup setup.
        # User would like to setup a separate evaluation worker set.
        # Note: We skip EnvRunnerGroup creation if we need to do offline evaluation.
        if self._should_create_evaluation_env_runners(self.evaluation_config):
            _, env_creator = self._get_env_id_and_creator(
                self.evaluation_config.env, self.evaluation_config
            )

            # Create a separate evaluation worker set for evaluation.
            # If evaluation_num_env_runners=0, use the evaluation set's local
            # worker for evaluation, otherwise, use its remote workers
            # (parallelized evaluation).
            self.eval_env_runner_group: EnvRunnerGroup = EnvRunnerGroup(
                env_creator=env_creator,
                validate_env=None,
                default_policy_class=self.get_default_policy_class(self.config),
                config=self.evaluation_config,
                logdir=self.logdir,
                tune_trial_id=self.trial_id,
                # New API stack: User decides whether to create local env runner.
                # Old API stack: Always create local EnvRunner.
                local_env_runner=(
                    True
                    if not self.evaluation_config.enable_env_runner_and_connector_v2
                    else self.evaluation_config.create_local_env_runner
                ),
                pg_offset=self.config.num_env_runners,
            )

        if self.env_runner_group:
            self.spaces = self.env_runner_group.get_spaces()
        elif self.eval_env_runner_group:
            self.spaces = self.eval_env_runner_group.get_spaces()

        if self.env_runner is None and self.spaces is not None:
            self.env_to_module_connector = self.config.build_env_to_module_connector(
                spaces=self.spaces
            )
            self.module_to_env_connector = self.config.build_module_to_env_connector(
                spaces=self.spaces
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

        if self.config.enable_rl_module_and_learner:
            spaces = {
                INPUT_ENV_SPACES: (
                    self.config.observation_space,
                    self.config.action_space,
                )
            }
            if self.env_runner_group:
                spaces.update(self.spaces)
            elif self.eval_env_runner_group:
                spaces.update(self.eval_env_runner_group.get_spaces())
            else:
                # If the algorithm is online we use the spaces from as they are
                # provided.
                if self.config.is_online:
                    spaces.update(
                        {
                            DEFAULT_MODULE_ID: (
                                self.config.observation_space,
                                self.config.action_space,
                            ),
                        }
                    )
                # Otherwise, when we are offline we need to check, if the learner connector
                # is transforming the spaces.
                elif self.config.is_offline:
                    # Build the learner connector with the input spaces from the environment.
                    learner_connector = self.config.build_learner_connector(
                        input_observation_space=spaces[INPUT_ENV_SPACES][0],
                        input_action_space=spaces[INPUT_ENV_SPACES][1],
                    )
                    # Update the `spaces` dictionary by using the output spaces of the learner
                    # connector pipeline.
                    spaces.update(
                        {
                            DEFAULT_MODULE_ID: (
                                learner_connector.observation_space,
                                learner_connector.action_space,
                            ),
                        }
                    )

            module_spec: MultiRLModuleSpec = self.config.get_multi_rl_module_spec(
                spaces=spaces,
                inference_only=False,
            )
            self.learner_group = self.config.build_learner_group(
                rl_module_spec=module_spec
            )

            # Check if there are modules to load from the `module_spec`.
            rl_module_ckpt_dirs = {}
            multi_rl_module_ckpt_dir = module_spec.load_state_path
            modules_to_load = module_spec.modules_to_load
            for module_id, sub_module_spec in module_spec.rl_module_specs.items():
                if sub_module_spec.load_state_path:
                    rl_module_ckpt_dirs[module_id] = sub_module_spec.load_state_path
            if multi_rl_module_ckpt_dir or rl_module_ckpt_dirs:
                self.learner_group.load_module_state(
                    multi_rl_module_ckpt_dir=multi_rl_module_ckpt_dir,
                    modules_to_load=modules_to_load,
                    rl_module_ckpt_dirs=rl_module_ckpt_dirs,
                )

            # Sync the weights from the learner group to the EnvRunners.
            rl_module_state = self.learner_group.get_state(
                components=COMPONENT_LEARNER + "/" + COMPONENT_RL_MODULE,
                inference_only=True,
            )[COMPONENT_LEARNER]
            if self.env_runner_group:
                self.env_runner_group.sync_env_runner_states(
                    config=self.config,
                    env_steps_sampled=self.metrics.peek(
                        (ENV_RUNNER_RESULTS, NUM_ENV_STEPS_SAMPLED_LIFETIME), default=0
                    ),
                    rl_module_state=rl_module_state,
                    env_to_module=self.env_to_module_connector,
                    module_to_env=self.module_to_env_connector,
                )
            elif self.eval_env_runner_group:
                self.eval_env_runner_group.sync_env_runner_states(
                    config=self.evaluation_config,
                    env_steps_sampled=self.metrics.peek(
                        (ENV_RUNNER_RESULTS, NUM_ENV_STEPS_SAMPLED_LIFETIME), default=0
                    ),
                    rl_module_state=rl_module_state,
                    env_to_module=self.env_to_module_connector,
                    module_to_env=self.module_to_env_connector,
                )
            # TODO (simon): Update modules in DataWorkers.

            if self.offline_data:
                # If the learners are remote we need to provide specific
                # information and the learner's actor handles.
                if self.learner_group.is_remote:
                    # If learners run on different nodes, locality hints help
                    # to use the nearest learner in the workers that do the
                    # data preprocessing.
                    learner_node_ids = self.learner_group.foreach_learner(
                        lambda _: ray.get_runtime_context().get_node_id()
                    )
                    self.offline_data.locality_hints = [
                        node_id.get() for node_id in learner_node_ids
                    ]
                    # Provide the actor handles for the learners for module
                    # updating during preprocessing.
                    self.offline_data.learner_handles = self.learner_group._workers
                # Otherwise we can simply pass in the local learner.
                else:
                    self.offline_data.learner_handles = [self.learner_group._learner]
                # TODO (simon, sven): Replace these set-some-object's-attributes-
                # directly? We should find some solution for this in the future, an API,
                # or setting these in the OfflineData constructor?
                # Provide the module_spec. Note, in the remote case this is needed
                # because the learner module cannot be copied, but must be built.
                self.offline_data.module_spec = module_spec
                # Provide the `OfflineData` instance with space information. It might
                # need it for reading recorded experiences.
                self.offline_data.spaces = spaces

            if self._should_create_offline_evaluation_runners(self.evaluation_config):
                from ray.rllib.offline.offline_evaluation_runner_group import (
                    OfflineEvaluationRunnerGroup,
                )

                # If no inference-only `RLModule` should be used in offline evaluation,
                # get the complete learner module.
                if not self.evaluation_config.offline_eval_rl_module_inference_only:
                    rl_module_state = self.learner_group.get_state(
                        components=COMPONENT_LEARNER + "/" + COMPONENT_RL_MODULE,
                        inference_only=False,
                    )[COMPONENT_LEARNER]
                # Create the offline evaluation runner group.
                self.offline_eval_runner_group: OfflineEvaluationRunnerGroup = OfflineEvaluationRunnerGroup(
                    config=self.evaluation_config,
                    # Do not create a local runner such that the dataset can be split.
                    local_runner=self.config.num_offline_eval_runners == 0,
                    # Provide the `RLModule`'s state for the `OfflinePreLearner`s.
                    module_state=rl_module_state[COMPONENT_RL_MODULE],
                    module_spec=module_spec,
                    # Note, even if no environment is run, the `MultiRLModule` needs
                    # spaces to construct the policy network.
                    spaces=spaces,
                )

        # Create an Aggregator actor set, if necessary.
        self._aggregator_actor_manager = None
        if self.config.enable_rl_module_and_learner and (
            self.config.num_aggregator_actors_per_learner > 0
        ):
            rl_module_spec = self.config.get_multi_rl_module_spec(
                spaces=self.spaces,
                inference_only=False,
            )
            agg_cls = ray.remote(
                num_cpus=1,
                max_restarts=-1,
            )(AggregatorActor)
            self._aggregator_actor_manager = FaultTolerantActorManager(
                [
                    agg_cls.remote(self.config, rl_module_spec)
                    for _ in range(
                        (self.config.num_learners or 1)
                        * self.config.num_aggregator_actors_per_learner
                    )
                ],
                max_remote_requests_in_flight_per_actor=(
                    self.config.max_requests_in_flight_per_aggregator_actor
                ),
            )
            # Get the devices of each learner.
            learner_locations = list(
                enumerate(
                    self.learner_group.foreach_learner(
                        func=lambda _learner: (_learner.node, _learner.device),
                    )
                )
            )
            # Get the devices of each AggregatorActor.
            aggregator_locations = list(
                enumerate(
                    self._aggregator_actor_manager.foreach_actor(
                        func=lambda actor: (actor._node, actor._device)
                    )
                )
            )
            self._aggregator_actor_to_learner = {}
            for agg_idx, aggregator_location in aggregator_locations:
                aggregator_location = aggregator_location.get()
                for learner_idx, learner_location in learner_locations:
                    # TODO (sven): Activate full comparison (including device) when Ray
                    #  has figured out GPU pre-loading.
                    if learner_location.get()[0] == aggregator_location[0]:
                        # Round-robin, in case all Learners are on same device/node.
                        learner_locations = learner_locations[1:] + [
                            learner_locations[0]
                        ]
                        self._aggregator_actor_to_learner[agg_idx] = learner_idx
                        break
                if agg_idx not in self._aggregator_actor_to_learner:
                    raise RuntimeError(
                        "No Learner worker found that matches aggregation worker "
                        f"#{agg_idx}'s node ({aggregator_location[0]}) and device "
                        f"({aggregator_location[1]})! The Learner workers' locations "
                        f"are {learner_locations}."
                    )

            # Make sure, each Learner index is mapped to from at least one
            # AggregatorActor.
            if not all(
                learner_idx in self._aggregator_actor_to_learner.values()
                for learner_idx in range(self.config.num_learners or 1)
            ):
                raise RuntimeError(
                    "Some Learner indices are not mapped to from any AggregatorActors! "
                    "Final AggregatorActor idx -> Learner idx mapping is: "
                    f"{self._aggregator_actor_to_learner}"
                )

        # Ray metrics
        self._set_up_metrics()

        # Run `on_algorithm_init` callback after initialization is done.
        make_callback(
            "on_algorithm_init",
            self.callbacks,
            self.config.callbacks_on_algorithm_init,
            kwargs=dict(
                algorithm=self,
                metrics_logger=self.metrics,
            ),
        )

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
        # Ray metrics
        with TimerAndPrometheusLogger(self._metrics_step_time):
            # Do we have to run `self.evaluate()` this iteration?
            # `self.iteration` gets incremented after this function returns,
            # meaning that e.g. the first time this function is called,
            # self.iteration will be 0.
            evaluate_this_iter = (
                self.config.evaluation_interval
                and (self.iteration + 1) % self.config.evaluation_interval == 0
            )

            evaluate_offline_this_iter = (
                self.config.offline_evaluation_interval
                and (self.iteration + 1) % self.config.offline_evaluation_interval == 0
            )

            # Results dict for training (and if appolicable: evaluation).
            eval_results: ResultDict = {}

            # Parallel eval + training: Kick off evaluation-loop and parallel train() call.
            if evaluate_this_iter and (
                self.config.evaluation_parallel_to_training
                or self.config.offline_evaluation_parallel_to_training
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
                if self.config.enable_env_runner_and_connector_v2:
                    train_results, train_iter_ctx = self._run_one_training_iteration()
                else:
                    (
                        train_results,
                        train_iter_ctx,
                    ) = self._run_one_training_iteration_old_api_stack()

            # Sequential: Train (already done above), then evaluate.
            if evaluate_this_iter and not self.config.evaluation_parallel_to_training:
                eval_results = self._run_one_evaluation(parallel_train_future=None)

            if evaluate_offline_this_iter:
                offline_eval_results = self._run_one_offline_evaluation()
                # If we already have online evaluation results merge the offline
                # evaluation results.
                if eval_results:
                    eval_results[EVALUATION_RESULTS].update(
                        offline_eval_results[EVALUATION_RESULTS]
                    )
                # Otherwise, just assign.
                else:
                    eval_results = offline_eval_results

            # Sync EnvRunner workers.
            # TODO (sven): For the new API stack, the common execution pattern for any algo
            #  should be: [sample + get_metrics + get_state] -> send all these in one remote
            #  call down to `training_step` (where episodes are sent as ray object
            #  references). Then distribute the episode refs to the learners, store metrics
            #  in special key in result dict and perform the connector merge/broadcast
            #  inside the `training_step` as well. See the new IMPALA for an example.
            if self.config.enable_env_runner_and_connector_v2:
                if (
                    not self.config._dont_auto_sync_env_runner_states
                    and self.env_runner_group
                ):
                    # Synchronize EnvToModule and ModuleToEnv connector states and broadcast
                    # new states back to all EnvRunners.
                    with self.metrics.log_time(
                        (TIMERS, SYNCH_ENV_CONNECTOR_STATES_TIMER)
                    ):
                        with TimerAndPrometheusLogger(
                            self._metrics_step_sync_env_runner_states_time
                        ):
                            self.env_runner_group.sync_env_runner_states(
                                config=self.config,
                                env_steps_sampled=self.metrics.peek(
                                    (
                                        ENV_RUNNER_RESULTS,
                                        NUM_ENV_STEPS_SAMPLED_LIFETIME,
                                    ),
                                    default=0,
                                ),
                                env_to_module=self.env_to_module_connector,
                                module_to_env=self.module_to_env_connector,
                            )
                # Compile final ResultDict from `train_results` and `eval_results`. Note
                # that, as opposed to the old API stack, EnvRunner stats should already be
                # in `train_results` and `eval_results`.
                results = self._compile_iteration_results(
                    train_results=train_results,
                    eval_results=eval_results,
                )
            else:
                self._sync_filters_if_needed(
                    central_worker=self.env_runner_group.local_env_runner,
                    workers=self.env_runner_group,
                    config=self.config,
                )
                # Get EnvRunner metrics and compile them into results.
                episodes_this_iter = collect_episodes(
                    self.env_runner_group,
                    self._remote_worker_ids_for_metrics(),
                    timeout_seconds=self.config.metrics_episode_collection_timeout_s,
                )
                results = self._compile_iteration_results_old_api_stack(
                    episodes_this_iter=episodes_this_iter,
                    step_ctx=train_iter_ctx,
                    iteration_results={**train_results, **eval_results},
                )

        return results

    @PublicAPI
    def evaluate_offline(self):
        """Evaluates current policy offline under `evaluation_config` settings.

        Returns:
            A ResultDict only containing the offline evaluation results from the current
            iteration.
        """

        # First synchronize weights.
        self.offline_eval_runner_group.sync_weights(
            from_worker_or_learner_group=self.learner_group,
            inference_only=self.config.offline_eval_rl_module_inference_only,
        )

        # TODO (simon): Check, how we can sync without a local runner. Also,
        # connectors are in the data pipeline not directly in the runner applied.
        # if self.config.broadcast_offline_eval_runner_states:
        #     # TODO (simon): Create offline equivalent.
        #     with self.metrics.log_time(TIMERS, SYNCH_EVAL_ENV_CONNECTOR_STATES_TIMER):
        #         self.offline_eval_runner_group.sync_runner_states(
        #             from_runner=
        #         )
        make_callback(
            "on_evaluate_offline_start",
            callbacks_objects=self.callbacks,
            callbacks_functions=self.config.callbacks_on_evaluate_offline_start,
            kwargs=dict(algorithm=self, metrics_logger=self.metrics),
        )

        # Evaluate with fixed duration.
        if self.offline_eval_runner_group.num_healthy_remote_runners > 0:
            self._evaluate_offline_with_fixed_duration()
        else:
            self._evaluate_offline_on_local_runner()
        # Reduce the evaluation results.
        eval_results = self.metrics.peek(
            (EVALUATION_RESULTS, OFFLINE_EVAL_RUNNER_RESULTS),
            default={},
            latest_merged_only=True,
        )

        # Trigger `on_evaluate_offline_end` callback.
        make_callback(
            "on_evaluate_offline_end",
            callbacks_objects=self.callbacks,
            callbacks_functions=self.config.callbacks_on_evaluate_offline_end,
            kwargs=dict(
                algorithm=self,
                metrics_logger=self.metrics,
                evaluation_metrics=eval_results,
            ),
        )

        # Also return the results here for convenience.
        return {OFFLINE_EVAL_RUNNER_RESULTS: eval_results}

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
        with TimerAndPrometheusLogger(self._metrics_evaluate_time):
            # Call the `_before_evaluate` hook.
            self._before_evaluate()

            if self.evaluation_dataset is not None:
                return self._run_offline_evaluation_old_api_stack()

            if self.config.enable_env_runner_and_connector_v2:
                if (
                    self.env_runner_group is not None
                    and self.env_runner_group.healthy_env_runner_ids()
                ):
                    # TODO (sven): Replace this with a new ActorManager API:
                    #  try_remote_request_till_success("get_state") -> tuple(int,
                    #  remoteresult)
                    weights_src = self.env_runner_group._worker_manager._actors[
                        self.env_runner_group.healthy_env_runner_ids()[0]
                    ]
                else:
                    weights_src = self.learner_group
            else:
                weights_src = self.env_runner

            # Sync weights to the evaluation EnvRunners.
            if self.eval_env_runner_group is not None:
                with TimerAndPrometheusLogger(
                    self._metrics_evaluate_sync_env_runner_weights_time
                ):
                    self.eval_env_runner_group.sync_weights(
                        from_worker_or_learner_group=weights_src,
                        inference_only=True,
                    )

                # Merge (eval) EnvRunner states and broadcast the merged state back
                # to the remote (eval) EnvRunner actors.
                if self.config.enable_env_runner_and_connector_v2:
                    if self.evaluation_config.broadcast_env_runner_states:
                        with self.metrics.log_time(
                            (TIMERS, SYNCH_EVAL_ENV_CONNECTOR_STATES_TIMER)
                        ):
                            with TimerAndPrometheusLogger(
                                self._metrics_evaluate_sync_connector_states_time
                            ):
                                self.eval_env_runner_group.sync_env_runner_states(
                                    config=self.evaluation_config,
                                    from_worker=self.env_runner,
                                    env_steps_sampled=self.metrics.peek(
                                        (
                                            ENV_RUNNER_RESULTS,
                                            NUM_ENV_STEPS_SAMPLED_LIFETIME,
                                        ),
                                        default=0,
                                    ),
                                    env_to_module=self.env_to_module_connector,
                                    module_to_env=self.module_to_env_connector,
                                )
                else:
                    self._sync_filters_if_needed(
                        central_worker=self.env_runner_group.local_env_runner,
                        workers=self.eval_env_runner_group,
                        config=self.evaluation_config,
                    )
            # Sync weights to the local EnvRunner (if no eval EnvRunnerGroup).
            elif self.config.enable_env_runner_and_connector_v2:
                self.env_runner_group.sync_weights(
                    from_worker_or_learner_group=weights_src,
                    inference_only=True,
                )

            with TimerAndPrometheusLogger(
                self._metrics_callback_on_evaluate_start_time
            ):
                make_callback(
                    "on_evaluate_start",
                    callbacks_objects=self.callbacks,
                    callbacks_functions=self.config.callbacks_on_evaluate_start,
                    kwargs=dict(algorithm=self, metrics_logger=self.metrics),
                )

            env_steps = agent_steps = 0
            batches = []

            # We will use a user provided evaluation function.
            if self.config.custom_evaluation_function:
                if self.config.enable_env_runner_and_connector_v2:
                    (
                        eval_results,
                        env_steps,
                        agent_steps,
                    ) = self._evaluate_with_custom_eval_function()
                else:
                    eval_results = self.config.custom_evaluation_function()
            # There is no eval EnvRunnerGroup -> Run on local EnvRunner.
            elif self.eval_env_runner_group is None and self.env_runner:
                (
                    eval_results,
                    env_steps,
                    agent_steps,
                    batches,
                ) = self._evaluate_on_local_env_runner(self.env_runner)
            # There is only a local eval EnvRunner -> Run on that.
            elif self.eval_env_runner_group.num_healthy_remote_workers() == 0:
                (
                    eval_results,
                    env_steps,
                    agent_steps,
                    batches,
                ) = self._evaluate_on_local_env_runner(self.eval_env_runner)
            # There are healthy remote evaluation workers -> Run on these.
            elif self.eval_env_runner_group.num_healthy_remote_workers() > 0:
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
                eval_results = {}

            if self.config.enable_env_runner_and_connector_v2:
                eval_results = self.metrics.peek(
                    key=EVALUATION_RESULTS,
                    default={},
                    latest_merged_only=True,
                )
                if log_once("no_eval_results") and not eval_results:
                    logger.warning(
                        "No evaluation results found for this iteration. This can happen if the evaluation worker(s) is/are not healthy."
                    )
            else:
                eval_results = {ENV_RUNNER_RESULTS: eval_results}
                eval_results[NUM_AGENT_STEPS_SAMPLED_THIS_ITER] = agent_steps
                eval_results[NUM_ENV_STEPS_SAMPLED_THIS_ITER] = env_steps
                eval_results["timesteps_this_iter"] = env_steps
                self._counters[
                    NUM_ENV_STEPS_SAMPLED_FOR_EVALUATION_THIS_ITER
                ] = env_steps

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
            with TimerAndPrometheusLogger(self._metrics_callback_on_evaluate_end_time):
                make_callback(
                    "on_evaluate_end",
                    callbacks_objects=self.callbacks,
                    callbacks_functions=self.config.callbacks_on_evaluate_end,
                    kwargs=dict(
                        algorithm=self,
                        metrics_logger=self.metrics,
                        evaluation_metrics=eval_results,
                    ),
                )

        # Also return the results here for convenience.
        return eval_results

    def _evaluate_with_custom_eval_function(self) -> Tuple[ResultDict, int, int]:
        logger.info(
            f"Evaluating current state of {self} using the custom eval function "
            f"{self.config.custom_evaluation_function}"
        )
        if self.config.enable_env_runner_and_connector_v2:
            (
                eval_results,
                env_steps,
                agent_steps,
            ) = self.config.custom_evaluation_function(self, self.eval_env_runner_group)
            if not env_steps or not agent_steps:
                raise ValueError(
                    "Custom eval function must return "
                    "`Tuple[ResultDict, int, int]` with `int, int` being "
                    f"`env_steps` and `agent_steps`! Got {env_steps}, {agent_steps}."
                )
        else:
            eval_results = self.config.custom_evaluation_function()
        if not eval_results or not isinstance(eval_results, dict):
            raise ValueError(
                "Custom eval function must return "
                f"dict of metrics! Got {eval_results}."
            )

        return eval_results, env_steps, agent_steps

    def _evaluate_offline_on_local_runner(self):
        # How many episodes/timesteps do we need to run?
        unit = "batches"
        duration = (
            self.config.offline_evaluation_duration
            * self.config.dataset_num_iters_per_eval_runner
        )

        logger.info(f"Evaluating current state of {self} for {duration} {unit}.")

        results = self.offline_eval_runner_group.local_runner.run()

        self.metrics.aggregate(
            [results],
            key=(EVALUATION_RESULTS, OFFLINE_EVAL_RUNNER_RESULTS),
        )

    def _evaluate_on_local_env_runner(self, env_runner):
        if hasattr(env_runner, "input_reader") and env_runner.input_reader is None:
            raise ValueError(
                "Can't evaluate on a local worker if this local worker does not have "
                "an environment!\nTry one of the following:"
                "\n1) Set `evaluation_interval` > 0 to force creating a separate "
                "evaluation EnvRunnerGroup.\n2) Set `create_local_env_runner=True` to "
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
            self.metrics.aggregate(
                [env_runner_results],
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
        num_healthy_workers = self.eval_env_runner_group.num_healthy_remote_workers()
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
        if self.config.enable_env_runner_and_connector_v2:
            train_mean_time = self.metrics.peek(
                (TIMERS, TRAINING_ITERATION_TIMER), default=0.0
            )
        else:
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
                throughput_estimate = self.metrics.peek(
                    (
                        EVALUATION_RESULTS,
                        ENV_RUNNER_RESULTS,
                        NUM_ENV_STEPS_SAMPLED_LIFETIME,
                    ),
                    throughput=True,
                    # Note (artur): Peeking throughputs of lifetime metrics results in a dictionary with both throughputs (since last restore and total).
                    # We only need the throughput since last restore here.
                    default={"throughput_since_last_restore": 0.0},
                )["throughput_since_last_restore"]
                _num = min(
                    # Clamp number of steps to take between a max and a min.
                    self.config.evaluation_auto_duration_max_env_steps_per_sample,
                    max(
                        self.config.evaluation_auto_duration_min_env_steps_per_sample,
                        (
                            # How much time do we have left?
                            (train_mean_time - (time.time() - t0))
                            # Multiply by our own (eval) throughput to get the timesteps
                            # to do (per worker).
                            * throughput_estimate
                            / num_healthy_workers
                        ),
                    ),
                )

                results = (
                    self.eval_env_runner_group.foreach_env_runner_async_fetch_ready(
                        func=_env_runner_remote,
                        kwargs={"num": _num, "round": _round, "iter": algo_iteration},
                        tag="_env_runner_remote",
                    )
                )

                for env_s, ag_s, metrics, iter in results:
                    # Ignore eval results kicked off in an earlier iteration.
                    # (those results would be outdated and thus misleading).
                    if iter != self.iteration:
                        continue
                    env_steps += env_s
                    agent_steps += ag_s
                    all_metrics.append(metrics)
                time.sleep(0.01)

            # Old API stack -> RolloutWorkers return batches.
            else:
                results = (
                    self.eval_env_runner_group.foreach_env_runner_async_fetch_ready(
                        func=lambda w: (w.sample(), w.get_metrics(), algo_iteration),
                        tag="env_runner_sample_and_get_metrics",
                    )
                )

                for batch, metrics, iter in results:
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
            num_healthy_workers = (
                self.eval_env_runner_group.num_healthy_remote_workers()
            )

        if num_healthy_workers == 0:
            logger.warning(
                "Calling `sample()` on your remote evaluation worker(s) "
                "resulted in all workers crashing! Make sure a) your environment is not"
                " too unstable, b) you have enough evaluation workers "
                "(`config.evaluation(evaluation_num_env_runners=...)`) to cover for "
                "occasional losses, and c) you use the `config.fault_tolerance("
                "restart_failed_env_runners=True)` setting."
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
            self.metrics.aggregate(
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

    def _evaluate_offline_with_fixed_duration(self) -> None:
        # How many batches do we need to run?
        num_workers = self.config.num_offline_eval_runners
        time_out = self.config.offline_evaluation_timeout_s

        def _offline_eval_runner_remote(runner, iter):

            metrics = runner.run()

            return metrics, iter

        all_metrics = []
        num_units_done = []

        # How many episodes have we run (across all eval workers)?
        num_units_done = 0
        num_healthy_workers = self.offline_eval_runner_group.num_healthy_remote_runners

        # TODO (simon): Note, agent steps might not be available, but only
        # module steps.

        t_last_result = time.time()
        _round = -1
        algo_iteration = self.iteration

        # In case all the remote evaluation workers die during a round of
        # evaluation, we need to stop.
        while num_healthy_workers > 0:
            units_left_to_do = (
                self.config.offline_evaluation_duration * num_workers - num_units_done
            )
            if units_left_to_do <= 0:
                break

            _round += 1

            self.offline_eval_runner_group.foreach_runner_async(
                func=functools.partial(
                    _offline_eval_runner_remote,
                    iter=algo_iteration,
                ),
            )
            results = self.offline_eval_runner_group.fetch_ready_async_reqs(
                return_obj_refs=False, timeout_seconds=0.01
            )
            # Make sure we properly time out if we have not received any results
            # for more than `time_out` seconds.
            time_now = time.time()
            if not results and time_now - t_last_result > time_out:
                break
            elif results:
                t_last_result = time_now
            for wid, (met, iter) in results:
                if iter != self.iteration:
                    continue
                all_metrics.append(met)
                # Note, the `dataset_num_iters_per_eval_runner` must be smaller than
                # `offline_evaluation_duration` // `num_offline_eval_runners`.
                num_units_done += (
                    met[ALL_MODULES][DATASET_NUM_ITERS_EVALUATED].peek()
                    if DATASET_NUM_ITERS_EVALUATED in met[ALL_MODULES]
                    else 0
                )

            # Update correct number of healthy remote workers.
            num_healthy_workers = (
                self.offline_eval_runner_group.num_healthy_remote_runners
            )

        if num_healthy_workers == 0:
            logger.warning(
                "Calling `run()` on your remote offline evaluation runner(s) "
                "resulted in all runners crashing! Make sure a) your dataset is not"
                " corrupted, b) you have enough offline evaluation runners "
                "(`config.evaluation(num_offline_eval_runners=...)`) to cover for "
                "occasional losses, and c) you use the `config.fault_tolerance("
                "restart_failed_offline_eval_runners=True)` setting."
            )

        self.metrics.aggregate(
            all_metrics,
            key=(EVALUATION_RESULTS, OFFLINE_EVAL_RUNNER_RESULTS),
        )

    def _evaluate_with_fixed_duration(self):
        # How many episodes/timesteps do we need to run?
        unit = self.config.evaluation_duration_unit
        eval_cfg = self.evaluation_config
        num_workers = self.config.evaluation_num_env_runners
        force_reset = self.config.evaluation_force_reset_envs_before_iteration
        time_out = self.config.evaluation_sample_timeout_s

        # Remote function used on healthy EnvRunners to sample, get metrics, and
        # step counts.
        def _env_runner_remote(worker, num, round, iter, _force_reset):
            # Sample AND get_metrics, but only return metrics (and steps actually taken)
            # to save time. Also return the iteration to check, whether we should
            # discard and outdated result (from a slow worker).
            episodes = worker.sample(
                num_timesteps=(
                    num[worker.worker_index] if unit == "timesteps" else None
                ),
                num_episodes=(num[worker.worker_index] if unit == "episodes" else None),
                force_reset=_force_reset and round == 0,
            )
            metrics = worker.get_metrics()
            env_steps = sum(e.env_steps() for e in episodes)
            agent_steps = sum(e.agent_steps() for e in episodes)
            return env_steps, agent_steps, metrics, iter

        all_metrics = []
        all_batches = []

        # How many episodes have we run (across all eval workers)?
        num_units_done = 0
        num_healthy_workers = self.eval_env_runner_group.num_healthy_remote_workers()

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

                results = (
                    self.eval_env_runner_group.foreach_env_runner_async_fetch_ready(
                        func=_env_runner_remote,
                        kwargs={
                            "num": _num,
                            "round": _round,
                            "iter": algo_iteration,
                            "_force_reset": force_reset,
                        },
                        tag="_env_runner_remote",
                    )
                )

                # Make sure we properly time out if we have not received any results
                # for more than `time_out` seconds.
                time_now = time.time()
                if not results and time_now - t_last_result > time_out:
                    break
                elif results:
                    t_last_result = time_now
                for env_s, ag_s, met, iter in results:
                    if iter != self.iteration:
                        continue
                    env_steps += env_s
                    agent_steps += ag_s
                    all_metrics.append(met)
                    num_units_done += (
                        (met[NUM_EPISODES].peek() if NUM_EPISODES in met else 0)
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
                        self.eval_env_runner_group.healthy_worker_ids()
                    )
                    if i * units_per_healthy_remote_worker < units_left_to_do
                ]

                results = (
                    self.eval_env_runner_group.foreach_env_runner_async_fetch_ready(
                        func=lambda w: (w.sample(), w.get_metrics(), algo_iteration),
                        remote_worker_ids=selected_eval_worker_ids,
                        tag="env_runner_sample_and_get_metrics",
                    )
                )
                # Make sure we properly time out if we have not received any results
                # for more than `time_out` seconds.
                time_now = time.time()
                if not results and time_now - t_last_result > time_out:
                    break
                elif results:
                    t_last_result = time_now
                for batch, metrics, iter in results:
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
            num_healthy_workers = (
                self.eval_env_runner_group.num_healthy_remote_workers()
            )

        if num_healthy_workers == 0:
            logger.warning(
                "Calling `sample()` on your remote evaluation worker(s) "
                "resulted in all workers crashing! Make sure a) your environment is not"
                " too unstable, b) you have enough evaluation workers "
                "(`config.evaluation(evaluation_num_env_runners=...)`) to cover for "
                "occasional losses, and c) you use the `config.fault_tolerance("
                "restart_failed_env_runners=True)` setting."
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
            self.metrics.aggregate(
                all_metrics,
                key=(EVALUATION_RESULTS, ENV_RUNNER_RESULTS),
            )
            num_episodes = self.metrics.peek(
                (EVALUATION_RESULTS, ENV_RUNNER_RESULTS, NUM_EPISODES),
                default=0,
                latest_merged_only=True,
            )
            env_runner_results = None

        # Warn if results are empty, it could be that this is because the eval timesteps
        # are not enough to run through one full episode.
        if num_episodes == 0:
            logger.warning(
                "This evaluation iteration resulted in an empty set of episode summary "
                "results! It's possible that your configured duration timesteps are not"
                " enough to finish even a single episode. You have configured "
                f"{self.config.evaluation_duration} "
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
    def restore_env_runners(self, env_runner_group: EnvRunnerGroup) -> List[int]:
        """Try bringing back unhealthy EnvRunners and - if successful - sync with local.

        Algorithms that use custom EnvRunners may override this method to
        disable the default, and create custom restoration logics. Note that "restoring"
        does not include the actual restarting process, but merely what should happen
        after such a restart of a (previously failed) worker.

        Args:
            env_runner_group: The EnvRunnerGroup to restore. This may be the training or
                the evaluation EnvRunnerGroup.

        Returns:
            A list of EnvRunner indices that have been restored during the call of
            this method.
        """
        # This is really cheap, since probe_unhealthy_env_runners() is a no-op
        # if there are no unhealthy workers.
        restored = None
        if self.config.is_online:
            restored = env_runner_group.probe_unhealthy_env_runners()

        if not restored:
            return []

        # Count the restored workers.
        self._counters["total_num_restored_workers"] += len(restored)

        from_env_runner = env_runner_group.local_env_runner or self.env_runner

        # Sync from local EnvRunner, if it exists.
        if from_env_runner is not None:
            # Get the state of the EnvRunner.
            state = from_env_runner.get_state()
            state_ref = ray.put(state)

            # Take out (old) connector states from local worker's state.
            if not self.config.enable_env_runner_and_connector_v2:
                for pol_states in state["policy_states"].values():
                    pol_states.pop("connector_configs", None)

            elif self.config.is_multi_agent:

                multi_rl_module_spec = MultiRLModuleSpec.from_module(
                    from_env_runner.module
                )

        # Otherwise, sync from another EnvRunner that's still healthy.
        else:
            multi_rl_module_spec = (
                self.learner_group.foreach_learner(
                    lambda learner: MultiRLModuleSpec.from_module(learner.module)
                )
                .result_or_errors[0]
                .get()
            )

            # Sync the weights from the learner group to the EnvRunners.
            state = self.learner_group.get_state(
                components=COMPONENT_LEARNER + "/" + COMPONENT_RL_MODULE,
                inference_only=True,
            )[COMPONENT_LEARNER]
            state[
                COMPONENT_ENV_TO_MODULE_CONNECTOR
            ] = self.env_to_module_connector.get_state()
            state[
                COMPONENT_MODULE_TO_ENV_CONNECTOR
            ] = self.module_to_env_connector.get_state()
            state[NUM_ENV_STEPS_SAMPLED_LIFETIME] = self.metrics.peek(
                (ENV_RUNNER_RESULTS, NUM_ENV_STEPS_SAMPLED_LIFETIME),
                default=0,
            )
            state_ref = ray.put(state)

        def _sync_env_runner(er):  # noqa
            # Remove modules (new API stack only), if necessary.
            if (
                er.config.enable_env_runner_and_connector_v2
                and er.config.is_multi_agent
            ):
                for module_id, module in er.module._rl_modules.copy().items():
                    if module_id not in multi_rl_module_spec.rl_module_specs:
                        er.module.remove_module(module_id, raise_err_if_not_found=True)
                # Add modules, if necessary.
                for mid, mod_spec in multi_rl_module_spec.rl_module_specs.items():
                    if mid not in er.module:
                        er.module.add_module(mid, mod_spec.build(), override=False)
            # Now that the MultiRLModule is fixed, update the state.
            er.set_state(ray.get(state_ref))

        # By default, entire local EnvRunner state is synced after restoration
        # to bring the previously failed EnvRunner up to date.
        env_runner_group.foreach_env_runner(
            func=_sync_env_runner,
            remote_worker_ids=restored,
            # Don't update the local EnvRunner, b/c it's the one we are synching
            # from.
            local_env_runner=False,
            timeout_seconds=self.config.env_runner_restore_timeout_s,
        )

        return restored

    @OverrideToImplementCustomLogic
    def restore_offline_eval_runners(self, runner_group: RunnerGroup) -> List[int]:
        if not runner_group or not runner_group.local_runner:
            return []

        restored = runner_group.probe_unhealthy_runners()

        if restored:
            # Count the restored workers.
            self._counters["total_num_restored_workers"] += len(restored)

            # Get the state of the correct (reference) worker.
            from_runner = runner_group.healthy_runner_ids()[0]
            state = runner_group.foreach_runner(
                "get_state",
                local_runner=False,
                remote_worker_ids=from_runner,
            )[0]
            state_ref = ray.put(state)

            def _sync_runner(r):
                r.set_state(ray.get(state_ref))

            # By default, entire `Runner`` state is synced after restoration
            # to bring the previously failed `Runner` up to date.
            runner_group.foreach_runner(
                func=_sync_runner,
                remote_worker_ids=restored,
                # Don't update the local `Runner`.
                local_runner=False,
                timeout_seconds=self.evaluation_config.offline_eval_runner_restore_timeout_s,
            )
            # Restore the correct data iterator split stream.
            # TODO (simon): Define a `restore` method in the `RunnerGroup`
            # such that we do not have to check here for the group.
            # Also get a different streaming split if a runner fails and is not
            # recreated.
            runner_group.foreach_runner(
                func="set_dataset_iterator",
                remote_worker_ids=restored,
                local_runner=False,
                timeout_seconds=self.evaluation_config.offline_eval_runner_restore_timeout_s,
                kwargs={"iterator": runner_group._offline_data_iterators[restored]},
            )

        return restored

    @OverrideToImplementCustomLogic
    def training_step(self) -> None:
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
            For the new API stack, returns None. Results are compiled and extracted
            automatically through a single `self.metrics.reduce()` call at the very end
            of an iteration (which might contain more than one call to
            `training_step()`). This way, we make sure that we account for all
            results generated by each individual `training_step()` call.
            For the old API stack, returns the results dict from executing the training
            step.
        """
        if not self.config.enable_env_runner_and_connector_v2:
            raise NotImplementedError(
                "The `Algorithm.training_step()` default implementation no longer "
                "supports the old API stack! If you would like to continue "
                "using these "
                "old APIs with this default `training_step`, simply subclass "
                "`Algorithm` and override its `training_step` method (copy/paste the "
                "code and delete this error message)."
            )

        # Collect a list of Episodes from EnvRunners until we reach the train batch
        # size.
        with self.metrics.log_time((TIMERS, ENV_RUNNER_SAMPLING_TIMER)):
            if self.config.count_steps_by == "agent_steps":
                episodes, env_runner_results = synchronous_parallel_sample(
                    worker_set=self.env_runner_group,
                    max_agent_steps=self.config.total_train_batch_size,
                    sample_timeout_s=self.config.sample_timeout_s,
                    _uses_new_env_runners=True,
                    _return_metrics=True,
                )
            else:
                episodes, env_runner_results = synchronous_parallel_sample(
                    worker_set=self.env_runner_group,
                    max_env_steps=self.config.total_train_batch_size,
                    sample_timeout_s=self.config.sample_timeout_s,
                    _uses_new_env_runners=True,
                    _return_metrics=True,
                )
        # Reduce EnvRunner metrics over the n EnvRunners.
        self.metrics.aggregate(env_runner_results, key=ENV_RUNNER_RESULTS)

        with self.metrics.log_time((TIMERS, LEARNER_UPDATE_TIMER)):
            learner_results = self.learner_group.update(
                episodes=episodes,
                timesteps={
                    NUM_ENV_STEPS_SAMPLED_LIFETIME: (
                        self.metrics.peek(NUM_ENV_STEPS_SAMPLED_LIFETIME)
                    ),
                },
            )
            self.metrics.aggregate(learner_results, key=LEARNER_RESULTS)

        # Update weights - after learning on the local worker - on all
        # remote workers (only those RLModules that were actually trained).
        with self.metrics.log_time((TIMERS, SYNCH_WORKER_WEIGHTS_TIMER)):
            self.env_runner_group.sync_weights(
                from_worker_or_learner_group=self.learner_group,
                policies=list(set(learner_results.keys()) - {ALL_MODULES}),
                inference_only=True,
            )

    @PublicAPI
    def get_module(self, module_id: ModuleID = DEFAULT_MODULE_ID) -> Optional[RLModule]:
        """Returns the (single-agent) RLModule with `model_id` (None if ID not found).

        Args:
            module_id: ID of the (single-agent) RLModule to return from the MARLModule
                used by the local EnvRunner.

        Returns:
            The RLModule found under the ModuleID key inside the local EnvRunner's
            MultiRLModule. None if `module_id` doesn't exist.
        """
        if self.env_runner is not None:
            module = self.env_runner.module
        else:
            module = self.env_runner_group.foreach_env_runner(
                lambda er: er.module,
                remote_worker_ids=[1],
                local_env_runner=False,
            )[0]

        if isinstance(module, MultiRLModule):
            return module.get(module_id)
        else:
            return module

    @PublicAPI
    def add_module(
        self,
        module_id: ModuleID,
        module_spec: RLModuleSpec,
        *,
        config_overrides: Optional[Dict] = None,
        new_agent_to_module_mapping_fn: Optional[AgentToModuleMappingFn] = None,
        new_should_module_be_updated: Optional[ShouldModuleBeUpdatedFn] = None,
        add_to_learners: bool = True,
        add_to_env_runners: bool = True,
        add_to_eval_env_runners: bool = True,
    ) -> MultiRLModuleSpec:
        """Adds a new (single-agent) RLModule to this Algorithm's MARLModule.

        Note that an Algorithm has up to 3 different components to which to add
        the new module to: The LearnerGroup (with n Learners), the EnvRunnerGroup
        (with m EnvRunners plus a local one) and - if applicable - the eval
        EnvRunnerGroup (with o EnvRunners plus a local one).

        Args:
            module_id: ID of the RLModule to add to the MARLModule.
                IMPORTANT: Must not contain characters that
                are also not allowed in Unix/Win filesystems, such as: `<>:"/|?*`,
                or a dot, space or backslash at the end of the ID.
            module_spec: The SingleAgentRLModuleSpec to use for constructing the new
                RLModule.
            config_overrides: The `AlgorithmConfig` overrides that should apply to
                the new Module, if any.
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
            add_to_learners: Whether to add the new RLModule to the LearnerGroup
                (with its n Learners).
            add_to_env_runners: Whether to add the new RLModule to the EnvRunnerGroup
                (with its m EnvRunners plus the local one).
            add_to_eval_env_runners: Whether to add the new RLModule to the eval
                EnvRunnerGroup (with its o EnvRunners plus the local one).

        Returns:
            The new MultiRLModuleSpec (after the RLModule has been added).
        """
        validate_module_id(module_id, error=True)

        # The to-be-returned new MultiRLModuleSpec.
        multi_rl_module_spec = None

        if not self.config.is_multi_agent:
            raise RuntimeError(
                "Can't add a new RLModule to a single-agent setup! Make sure that your "
                "setup is already initially multi-agent by either defining >1 "
                f"RLModules in your `rl_module_spec` or assigning a ModuleID other "
                f"than {DEFAULT_MODULE_ID} to your (only) RLModule."
            )

        if not any([add_to_learners, add_to_env_runners, add_to_eval_env_runners]):
            raise ValueError(
                "At least one of `add_to_learners`, `add_to_env_runners`, or "
                "`add_to_eval_env_runners` must be set to True!"
            )

        # Add to Learners and sync weights.
        if add_to_learners:
            multi_rl_module_spec = self.learner_group.add_module(
                module_id=module_id,
                module_spec=module_spec,
                config_overrides=config_overrides,
                new_should_module_be_updated=new_should_module_be_updated,
            )

        # Change our config (AlgorithmConfig) to contain the new Module.
        # TODO (sven): This is a hack to manipulate the AlgorithmConfig directly,
        #  but we'll deprecate config.policies soon anyway.
        self.config._is_frozen = False
        self.config.policies[module_id] = PolicySpec()
        if config_overrides is not None:
            self.config.multi_agent(
                algorithm_config_overrides_per_module={module_id: config_overrides}
            )
        if new_agent_to_module_mapping_fn is not None:
            self.config.multi_agent(policy_mapping_fn=new_agent_to_module_mapping_fn)
        self.config.rl_module(rl_module_spec=multi_rl_module_spec)
        if new_should_module_be_updated is not None:
            self.config.multi_agent(policies_to_train=new_should_module_be_updated)
        self.config.freeze()

        def _add(_env_runner, _module_spec=module_spec):
            # Add the RLModule to the existing one on the EnvRunner.
            _env_runner.module.add_module(
                module_id=module_id, module=_module_spec.build()
            )
            # Update the `agent_to_module_mapping_fn` on the EnvRunner.
            if new_agent_to_module_mapping_fn is not None:
                _env_runner.config.multi_agent(
                    policy_mapping_fn=new_agent_to_module_mapping_fn,
                )
            # Update the `should_module_be_updated` on the EnvRunner. Note that
            # even though this information is typically not needed by the EnvRunner,
            # it's good practice to keep this setting updated everywhere either way.
            if new_should_module_be_updated is not None:
                _env_runner.config.multi_agent(
                    policies_to_train=new_should_module_be_updated,
                )
            return MultiRLModuleSpec.from_module(_env_runner.module)

        # Add to (training) EnvRunners and sync weights.
        if add_to_env_runners:
            if multi_rl_module_spec is None:
                multi_rl_module_spec = self.env_runner_group.foreach_env_runner(_add)[0]
            else:
                self.env_runner_group.foreach_env_runner(_add)
            self.env_runner_group.sync_weights(
                from_worker_or_learner_group=self.learner_group,
                inference_only=True,
            )
        # Add to eval EnvRunners and sync weights.
        if add_to_eval_env_runners is True and self.eval_env_runner_group is not None:
            if multi_rl_module_spec is None:
                multi_rl_module_spec = self.eval_env_runner_group.foreach_env_runner(
                    _add
                )[0]
            else:
                self.eval_env_runner_group.foreach_env_runner(_add)
            self.eval_env_runner_group.sync_weights(
                from_worker_or_learner_group=self.learner_group,
                inference_only=True,
            )

        return multi_rl_module_spec

    @PublicAPI
    def remove_module(
        self,
        module_id: ModuleID,
        *,
        new_agent_to_module_mapping_fn: Optional[AgentToModuleMappingFn] = None,
        new_should_module_be_updated: Optional[ShouldModuleBeUpdatedFn] = None,
        remove_from_learners: bool = True,
        remove_from_env_runners: bool = True,
        remove_from_eval_env_runners: bool = True,
    ) -> Optional[Policy]:
        """Removes a new (single-agent) RLModule from this Algorithm's MARLModule.

        Args:
            module_id: ID of the RLModule to remove from the MARLModule.
                IMPORTANT: Must not contain characters that
                are also not allowed in Unix/Win filesystems, such as: `<>:"/|?*`,
                or a dot, space or backslash at the end of the ID.
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
            remove_from_learners: Whether to remove the RLModule from the LearnerGroup
                (with its n Learners).
            remove_from_env_runners: Whether to remove the RLModule from the
                EnvRunnerGroup (with its m EnvRunners plus the local one).
            remove_from_eval_env_runners: Whether to remove the RLModule from the eval
                EnvRunnerGroup (with its o EnvRunners plus the local one).

        Returns:
            The new MultiRLModuleSpec (after the RLModule has been removed).
        """
        # The to-be-returned new MultiRLModuleSpec.
        multi_rl_module_spec = None

        # Remove RLModule from the LearnerGroup.
        if remove_from_learners:
            multi_rl_module_spec = self.learner_group.remove_module(
                module_id=module_id,
                new_should_module_be_updated=new_should_module_be_updated,
            )

        # Change our config (AlgorithmConfig) with the Module removed.
        # TODO (sven): This is a hack to manipulate the AlgorithmConfig directly,
        #  but we'll deprecate config.policies soon anyway.
        self.config._is_frozen = False
        del self.config.policies[module_id]
        self.config.algorithm_config_overrides_per_module.pop(module_id, None)
        if new_agent_to_module_mapping_fn is not None:
            self.config.multi_agent(policy_mapping_fn=new_agent_to_module_mapping_fn)
        self.config.rl_module(rl_module_spec=multi_rl_module_spec)
        if new_should_module_be_updated is not None:
            self.config.multi_agent(policies_to_train=new_should_module_be_updated)
        self.config.freeze()

        def _remove(_env_runner):
            # Remove the RLModule from the existing one on the EnvRunner.
            _env_runner.module.remove_module(module_id=module_id)
            # Update the `agent_to_module_mapping_fn` on the EnvRunner.
            if new_agent_to_module_mapping_fn is not None:
                _env_runner.config.multi_agent(
                    policy_mapping_fn=new_agent_to_module_mapping_fn
                )
            # Force reset all ongoing episodes on the EnvRunner to avoid having
            # different ModuleIDs compute actions for the same AgentID in the same
            # episode.
            # TODO (sven): Create an API for this.
            _env_runner._needs_initial_reset = True

            return MultiRLModuleSpec.from_module(_env_runner.module)

        # Remove from (training) EnvRunners and sync weights.
        if remove_from_env_runners:
            if multi_rl_module_spec is None:
                multi_rl_module_spec = self.env_runner_group.foreach_env_runner(
                    _remove
                )[0]
            else:
                self.env_runner_group.foreach_env_runner(_remove)
            self.env_runner_group.sync_weights(
                from_worker_or_learner_group=self.learner_group,
                inference_only=True,
            )

        # Remove from (eval) EnvRunners and sync weights.
        if (
            remove_from_eval_env_runners is True
            and self.eval_env_runner_group is not None
        ):
            if multi_rl_module_spec is None:
                multi_rl_module_spec = self.eval_env_runner_group.foreach_env_runner(
                    _remove
                )[0]
            else:
                self.eval_env_runner_group.foreach_env_runner(_remove)
            self.eval_env_runner_group.sync_weights(
                from_worker_or_learner_group=self.learner_group,
                inference_only=True,
            )

        return multi_rl_module_spec

    @OldAPIStack
    def get_policy(self, policy_id: PolicyID = DEFAULT_POLICY_ID) -> Policy:
        """Return policy for the specified id, or None.

        Args:
            policy_id: ID of the policy to return.
        """
        return self.env_runner.get_policy(policy_id)

    @PublicAPI
    def get_weights(self, policies: Optional[List[PolicyID]] = None) -> dict:
        """Return a dict mapping Module/Policy IDs to weights.

        Args:
            policies: Optional list of policies to return weights for,
                or None for all policies.
        """
        # New API stack (get weights from LearnerGroup).
        if self.learner_group is not None:
            return self.learner_group.get_weights(module_ids=policies)
        return self.env_runner.get_weights(policies)

    @PublicAPI
    def set_weights(self, weights: Dict[PolicyID, dict]):
        """Set RLModule/Policy weights by Module/Policy ID.

        Args:
            weights: Dict mapping ModuleID/PolicyID to weights.
        """
        # New API stack -> Use `set_state` API and specify the LearnerGroup state in the
        # call, which will automatically take care of weight synching to all EnvRunners.
        if self.learner_group is not None:
            self.set_state(
                {
                    COMPONENT_LEARNER_GROUP: {
                        COMPONENT_LEARNER: {
                            COMPONENT_RL_MODULE: weights,
                        },
                    },
                },
            )
        self.env_runner_group.local_env_runner.set_weights(weights)

    @OldAPIStack
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
                Collection[PolicyID],
                Callable[[PolicyID, Optional[SampleBatchType]], bool],
            ]
        ] = None,
        add_to_env_runners: bool = True,
        add_to_eval_env_runners: bool = True,
        module_spec: Optional[RLModuleSpec] = None,
        # Deprecated arg.
        evaluation_workers=DEPRECATED_VALUE,
        add_to_learners=DEPRECATED_VALUE,
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
            add_to_env_runners: Whether to add the new RLModule to the EnvRunnerGroup
                (with its m EnvRunners plus the local one).
            add_to_eval_env_runners: Whether to add the new RLModule to the eval
                EnvRunnerGroup (with its o EnvRunners plus the local one).
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

        if evaluation_workers != DEPRECATED_VALUE:
            deprecation_warning(
                old="Algorithm.add_policy(evaluation_workers=...)",
                new="Algorithm.add_policy(add_to_eval_env_runners=...)",
                error=True,
            )
        if add_to_learners != DEPRECATED_VALUE:
            deprecation_warning(
                old="Algorithm.add_policy(add_to_learners=..)",
                help="Hybrid API stack no longer supported by RLlib!",
                error=True,
            )

        validate_module_id(policy_id, error=True)

        if add_to_env_runners is True:
            self.env_runner_group.add_policy(
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

        # Add to evaluation workers, if necessary.
        if add_to_eval_env_runners is True and self.eval_env_runner_group is not None:
            self.eval_env_runner_group.add_policy(
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

        # Return newly added policy (from the local EnvRunner).
        if add_to_env_runners:
            return self.get_policy(policy_id)
        elif add_to_eval_env_runners and self.eval_env_runner_group:
            return self.eval_env_runner.policy_map[policy_id]

    @OldAPIStack
    def remove_policy(
        self,
        policy_id: PolicyID = DEFAULT_POLICY_ID,
        *,
        policy_mapping_fn: Optional[Callable[[AgentID], PolicyID]] = None,
        policies_to_train: Optional[
            Union[
                Collection[PolicyID],
                Callable[[PolicyID, Optional[SampleBatchType]], bool],
            ]
        ] = None,
        remove_from_env_runners: bool = True,
        remove_from_eval_env_runners: bool = True,
        # Deprecated args.
        evaluation_workers=DEPRECATED_VALUE,
        remove_from_learners=DEPRECATED_VALUE,
    ) -> None:
        """Removes a policy from this Algorithm.

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
            remove_from_env_runners: Whether to remove the Policy from the
                EnvRunnerGroup (with its m EnvRunners plus the local one).
            remove_from_eval_env_runners: Whether to remove the RLModule from the eval
                EnvRunnerGroup (with its o EnvRunners plus the local one).
        """
        if evaluation_workers != DEPRECATED_VALUE:
            deprecation_warning(
                old="Algorithm.remove_policy(evaluation_workers=...)",
                new="Algorithm.remove_policy(remove_from_eval_env_runners=...)",
                error=False,
            )
            remove_from_eval_env_runners = evaluation_workers
        if remove_from_learners != DEPRECATED_VALUE:
            deprecation_warning(
                old="Algorithm.remove_policy(remove_from_learners=..)",
                help="Hybrid API stack no longer supported by RLlib!",
                error=True,
            )

        def fn(worker):
            worker.remove_policy(
                policy_id=policy_id,
                policy_mapping_fn=policy_mapping_fn,
                policies_to_train=policies_to_train,
            )

        # Update all EnvRunner workers.
        if remove_from_env_runners:
            self.env_runner_group.foreach_env_runner(fn, local_env_runner=True)

        # Update the evaluation worker set's workers, if required.
        if remove_from_eval_env_runners and self.eval_env_runner_group is not None:
            self.eval_env_runner_group.foreach_env_runner(fn, local_env_runner=True)

    @OldAPIStack
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
            KeyError: if `policy_id` cannot be found in this Algorithm.
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
        with TimerAndPrometheusLogger(self._metrics_save_checkpoint_time):
            # New API stack: Delegate to the `Checkpointable` implementation of
            # `save_to_path()` and return.
            if self.config.enable_rl_module_and_learner:
                self.save_to_path(
                    checkpoint_dir,
                    use_msgpack=self.config._use_msgpack_checkpoints,
                )
                return

            checkpoint_dir = pathlib.Path(checkpoint_dir)

            state = self.__getstate__()

            # Extract policy states from worker state (Policies get their own
            # checkpoint sub-dirs).
            policy_states = {}
            if "worker" in state and "policy_states" in state["worker"]:
                policy_states = state["worker"].pop("policy_states", {})

            # Add RLlib checkpoint version.
            if self.config.enable_rl_module_and_learner:
                state["checkpoint_version"] = CHECKPOINT_VERSION_LEARNER_AND_ENV_RUNNER
            else:
                state["checkpoint_version"] = CHECKPOINT_VERSION

            # Write state (w/o policies) to disk.
            state_file = checkpoint_dir / "algorithm_state.pkl"
            with open(state_file, "wb") as f:
                pickle.dump(state, f)

            # Write rllib_checkpoint.json.
            with open(checkpoint_dir / "rllib_checkpoint.json", "w") as f:
                json.dump(
                    {
                        "type": "Algorithm",
                        "checkpoint_version": str(state["checkpoint_version"]),
                        "format": "cloudpickle",
                        "state_file": str(state_file),
                        "policy_ids": list(policy_states.keys()),
                        "ray_version": ray.__version__,
                        "ray_commit": ray.__commit__,
                    },
                    f,
                )

            # Old API stack: Write individual policies to disk, each in their own
            # sub-directory.
            for pid, policy_state in policy_states.items():
                # From here on, disallow policyIDs that would not work as directory names.
                validate_module_id(pid, error=True)
                policy_dir = checkpoint_dir / "policies" / pid
                os.makedirs(policy_dir, exist_ok=True)
                policy = self.get_policy(pid)
                policy.export_checkpoint(policy_dir, policy_state=policy_state)

            # If we are using the learner API (hybrid API stack) -> Save the learner group's
            # state inside a "learner" subdir. Note that this is not in line with the
            # new Checkpointable API, but makes this case backward compatible.
            # The new Checkpointable API is only strictly applied anyways to the
            # new API stack.
            if self.config.enable_rl_module_and_learner:
                learner_state_dir = os.path.join(checkpoint_dir, "learner")
                self.learner_group.save_to_path(learner_state_dir)

    @override(Trainable)
    def load_checkpoint(self, checkpoint_dir: str) -> None:
        with TimerAndPrometheusLogger(self._metrics_load_checkpoint_time):
            # New API stack: Delegate to the `Checkpointable` implementation of
            # `restore_from_path()`.
            if self.config.enable_rl_module_and_learner:
                self.restore_from_path(checkpoint_dir)
            else:
                # Checkpoint is provided as a local directory.
                # Restore from the checkpoint file or dir.
                checkpoint_info = get_checkpoint_info(checkpoint_dir)
                checkpoint_data = Algorithm._checkpoint_info_to_algorithm_state(
                    checkpoint_info
                )
                self.__setstate__(checkpoint_data)

            # Call the `on_checkpoint_loaded` callback.
            make_callback(
                "on_checkpoint_loaded",
                callbacks_objects=self.callbacks,
                callbacks_functions=self.config.callbacks_on_checkpoint_loaded,
                kwargs=dict(algorithm=self),
            )

    @override(Checkpointable)
    def get_state(
        self,
        components: Optional[Union[str, Collection[str]]] = None,
        *,
        not_components: Optional[Union[str, Collection[str]]] = None,
        **kwargs,
    ) -> StateDict:
        if not self.config.enable_env_runner_and_connector_v2:
            raise RuntimeError(
                "Algorithm.get_state() not supported on the old API stack! "
                "Use Algorithm.__getstate__() instead."
            )

        state = {}

        # Get (local) EnvRunner state (w/o RLModule).
        if self.config.is_online:
            if self.env_runner:
                if self._check_component(
                    COMPONENT_ENV_RUNNER, components, not_components
                ):
                    state[COMPONENT_ENV_RUNNER] = self.env_runner.get_state(
                        components=self._get_subcomponents(
                            COMPONENT_RL_MODULE, components
                        ),
                        not_components=force_list(
                            self._get_subcomponents(COMPONENT_RL_MODULE, not_components)
                        )
                        # We don't want the RLModule state from the EnvRunners (it's
                        # `inference_only` anyway and already provided in full by the
                        # Learners).
                        + [COMPONENT_RL_MODULE],
                        **kwargs,
                    )
            else:
                if self._check_component(
                    COMPONENT_ENV_TO_MODULE_CONNECTOR, components, not_components
                ):
                    state[
                        COMPONENT_ENV_TO_MODULE_CONNECTOR
                    ] = self.env_to_module_connector.get_state()
                if self._check_component(
                    COMPONENT_MODULE_TO_ENV_CONNECTOR, components, not_components
                ):
                    state[
                        COMPONENT_MODULE_TO_ENV_CONNECTOR
                    ] = self.module_to_env_connector.get_state()
        # Get (local) evaluation EnvRunner state (w/o RLModule).
        if self.eval_env_runner and self._check_component(
            COMPONENT_EVAL_ENV_RUNNER, components, not_components
        ):
            state[COMPONENT_EVAL_ENV_RUNNER] = self.eval_env_runner.get_state(
                components=self._get_subcomponents(COMPONENT_RL_MODULE, components),
                not_components=force_list(
                    self._get_subcomponents(COMPONENT_RL_MODULE, not_components)
                )
                # We don't want the RLModule state from the EnvRunners (it's
                # `inference_only` anyway and already provided in full by the Learners).
                + [COMPONENT_RL_MODULE],
                **kwargs,
            )

        # Get LearnerGroup state (w/ RLModule).
        if self._check_component(COMPONENT_LEARNER_GROUP, components, not_components):
            state[COMPONENT_LEARNER_GROUP] = self.learner_group.get_state(
                components=self._get_subcomponents(COMPONENT_LEARNER_GROUP, components),
                not_components=self._get_subcomponents(
                    COMPONENT_LEARNER_GROUP, not_components
                ),
                **kwargs,
            )

        # Get entire MetricsLogger state.
        # TODO (sven): Make `MetricsLogger` a Checkpointable.
        state[COMPONENT_METRICS_LOGGER] = self.metrics.get_state()

        # Save current `training_iteration`.
        state[TRAINING_ITERATION] = self.training_iteration

        return state

    @override(Checkpointable)
    def set_state(self, state: StateDict) -> None:
        # Set the (training) EnvRunners' states.
        if COMPONENT_ENV_RUNNER in state:
            if self.env_runner:
                self.env_runner.set_state(state[COMPONENT_ENV_RUNNER])
            else:
                self.env_to_module_connector.set_state(
                    state[COMPONENT_ENV_RUNNER][COMPONENT_ENV_TO_MODULE_CONNECTOR]
                )
                self.module_to_env_connector.set_state(
                    state[COMPONENT_ENV_RUNNER][COMPONENT_MODULE_TO_ENV_CONNECTOR]
                )
            self.env_runner_group.sync_env_runner_states(
                config=self.config,
                from_worker=self.env_runner,
                env_steps_sampled=self.metrics.peek(
                    (ENV_RUNNER_RESULTS, NUM_ENV_STEPS_SAMPLED_LIFETIME), default=0
                ),
                env_to_module=self.env_to_module_connector,
                module_to_env=self.module_to_env_connector,
            )

        # Set the (eval) EnvRunners' states.
        if self.eval_env_runner_group and COMPONENT_EVAL_ENV_RUNNER in state:
            if self.eval_env_runner:
                self.eval_env_runner.set_state(state[COMPONENT_ENV_RUNNER])
            self.eval_env_runner_group.sync_env_runner_states(
                config=self.evaluation_config,
                from_worker=self.env_runner,
                env_steps_sampled=self.metrics.peek(
                    (ENV_RUNNER_RESULTS, NUM_ENV_STEPS_SAMPLED_LIFETIME), default=0
                ),
                env_to_module=self.env_to_module_connector,
                module_to_env=self.module_to_env_connector,
            )

        # Set the LearnerGroup's state.
        if COMPONENT_LEARNER_GROUP in state:
            self.learner_group.set_state(state[COMPONENT_LEARNER_GROUP])
            # Sync new weights to all EnvRunners.
            self.env_runner_group.sync_weights(
                from_worker_or_learner_group=self.learner_group,
                inference_only=True,
            )
            if self.eval_env_runner_group:
                self.eval_env_runner_group.sync_weights(
                    from_worker_or_learner_group=self.learner_group,
                    inference_only=True,
                )

        # TODO (sven): Make `MetricsLogger` a Checkpointable.
        if COMPONENT_METRICS_LOGGER in state:
            self.metrics.set_state(state[COMPONENT_METRICS_LOGGER])

        if TRAINING_ITERATION in state:
            self._iteration = state[TRAINING_ITERATION]

    @override(Checkpointable)
    def get_checkpointable_components(self) -> List[Tuple[str, "Checkpointable"]]:
        components = [
            (COMPONENT_LEARNER_GROUP, self.learner_group),
        ]
        if self.config.is_online and self.env_runner:
            components.append(
                (COMPONENT_ENV_RUNNER, self.env_runner),
            )
        elif self.config.is_online and not self.env_runner:
            if self.env_to_module_connector:
                components.append(
                    (COMPONENT_ENV_TO_MODULE_CONNECTOR, self.env_to_module_connector),
                )
            if self.module_to_env_connector:
                components.append(
                    (COMPONENT_MODULE_TO_ENV_CONNECTOR, self.module_to_env_connector),
                )
        if self.eval_env_runner:
            components.append(
                (
                    COMPONENT_EVAL_ENV_RUNNER,
                    self.eval_env_runner,
                )
            )
        return components

    @override(Checkpointable)
    def get_ctor_args_and_kwargs(self) -> Tuple[Tuple, Dict[str, Any]]:
        return (
            (self.config.get_state(),),  # *args,
            {},  # **kwargs
        )

    @override(Checkpointable)
    def restore_from_path(self, path, *args, **kwargs):
        # Override from parent method, b/c we might have to sync the EnvRunner weights
        # after having restored/loaded the LearnerGroup state.
        super().restore_from_path(path, *args, **kwargs)

        # Sync EnvRunners, if LearnerGroup's checkpoint can be found in path
        # or user loaded a subcomponent within the LearnerGroup (for example a module).
        path = pathlib.Path(path)
        if (path / COMPONENT_LEARNER_GROUP).is_dir() or (
            "component" in kwargs and COMPONENT_LEARNER_GROUP in kwargs["component"]
        ):
            # Make also sure, all (training) EnvRunners get the just loaded weights, but
            # only the inference-only ones.
            self.env_runner_group.sync_weights(
                from_worker_or_learner_group=self.learner_group,
                inference_only=True,
            )

        # If we have remote `EnvRunner`s but no local `EnvRunner` we have to restore states
        # from path.
        if self.env_runner_group.num_remote_env_runners() > 0 and not self.env_runner:
            if (path / COMPONENT_ENV_TO_MODULE_CONNECTOR).is_dir():
                self.env_to_module_connector.restore_from_path(
                    path / COMPONENT_ENV_TO_MODULE_CONNECTOR, *args, **kwargs
                )

            if (path / COMPONENT_MODULE_TO_ENV_CONNECTOR).is_dir():
                self.module_to_env_connector.restore_from_path(
                    path / COMPONENT_MODULE_TO_ENV_CONNECTOR, *args, **kwargs
                )

            self.env_runner_group.sync_env_runner_states(
                config=self.config,
                from_worker=None,
                env_steps_sampled=self.metrics.peek(
                    (ENV_RUNNER_RESULTS, NUM_ENV_STEPS_SAMPLED_LIFETIME), default=0
                ),
                # connector_states=connector_states,
                env_to_module=self.env_to_module_connector,
                module_to_env=self.module_to_env_connector,
            )
        # Otherwise get the connector states from the local `EnvRunner`.
        elif self.env_runner_group.num_remote_env_runners() > 0 and self.env_runner:
            self.env_runner_group.sync_env_runner_states(
                config=self.config,
                env_steps_sampled=self.metrics.peek(
                    (ENV_RUNNER_RESULTS, NUM_ENV_STEPS_SAMPLED_LIFETIME), default=0
                ),
                from_worker=self.env_runner,
            )

    @override(Trainable)
    def log_result(self, result: ResultDict) -> None:
        # Log after the callback is invoked, so that the user has a chance
        # to mutate the result.
        # TODO (sven): It might not make sense to pass in the MetricsLogger at this late
        #  point in time. In here, the result dict has already been "compiled" (reduced)
        #  by the MetricsLogger and there is probably no point in adding more Stats
        #  here.
        with TimerAndPrometheusLogger(self._metrics_callback_on_train_result_time):
            make_callback(
                "on_train_result",
                callbacks_objects=self.callbacks,
                callbacks_functions=self.config.callbacks_on_train_result,
                kwargs=dict(
                    algorithm=self,
                    metrics_logger=self.metrics,
                    result=result,
                ),
            )
        # Then log according to Trainable's logging logic.
        Trainable.log_result(self, result)

    @override(Trainable)
    def cleanup(self) -> None:
        # Stop all Learners.
        if hasattr(self, "learner_group") and self.learner_group is not None:
            self.learner_group.shutdown()

        # Stop all aggregation actors.
        if hasattr(self, "_aggregator_actor_manager") and (
            self._aggregator_actor_manager is not None
        ):
            self._aggregator_actor_manager.clear()

        # Stop all EnvRunners.
        if hasattr(self, "env_runner_group") and self.env_runner_group is not None:
            self.env_runner_group.stop()
        if (
            hasattr(self, "eval_env_runner_group")
            and self.eval_env_runner_group is not None
        ):
            self.eval_env_runner_group.stop()

    @OverrideToImplementCustomLogic
    @classmethod
    @override(Trainable)
    def default_resource_request(
        cls, config: Union[AlgorithmConfig, PartialAlgorithmConfigDict]
    ) -> Union[Resources, PlacementGroupFactory]:
        config = cls.get_default_config().update_from_dict(config)
        config.validate()
        config.freeze()
        eval_config = config.get_evaluation_config_object()
        eval_config.validate()
        eval_config.freeze()

        if config.enable_rl_module_and_learner:
            main_process = _get_main_process_bundle(config)
        else:
            main_process = {
                "CPU": config.num_cpus_for_main_process,
                "GPU": (
                    0
                    if config._fake_gpus
                    else config.num_gpus
                    if not config.enable_rl_module_and_learner
                    else 0
                ),
            }

        env_runner_bundles = _get_env_runner_bundles(config)

        if cls._should_create_evaluation_env_runners(eval_config):
            eval_env_runner_bundles = _get_env_runner_bundles(eval_config)
        else:
            eval_env_runner_bundles = []

        if cls._should_create_offline_evaluation_runners(eval_config):
            offline_eval_runner_bundles = _get_offline_eval_runner_bundles(eval_config)
        else:
            offline_eval_runner_bundles = []

        learner_bundles = []
        if config.enable_rl_module_and_learner:
            learner_bundles = _get_learner_bundles(config)

        bundles = (
            [main_process]
            + env_runner_bundles
            + eval_env_runner_bundles
            + offline_eval_runner_bundles
            + learner_bundles
        )

        return PlacementGroupFactory(
            bundles=bundles,
            strategy=config.placement_strategy,
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
            "`AlgorithmConfig.env_runners("
            "num_env_runners=.., num_cpus_per_env_runner=.., "
            "num_gpus_per_env_runner=.., ..)` and "
            "`AgorithmConfig.learners(num_learners=.., num_gpus_per_learner=..)`. See "
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
                "a class extending rllib.callbacks.callbacks.RLlibCallback; "
                "see `rllib/examples/metrics/custom_metrics_and_callbacks.py` for an "
                "example.",
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
            Exception: in case something is wrong with the given environment.
        """
        pass

    def _run_one_training_iteration(self) -> Tuple[ResultDict, "TrainIterCtx"]:
        """Runs one training iteration (`self.iteration` will be +1 after this).

        Calls `self.training_step()` repeatedly until the configured minimum time (sec),
        minimum sample- or minimum training steps have been reached.

        Returns:
            The ResultDict from the last call to `training_step()`. Note that even
            though we only return the last ResultDict, the user still has full control
            over the history and reduce behavior of individual metrics at the time these
            metrics are logged with `self.metrics.log_...()`.
        """
        with TimerAndPrometheusLogger(self._metrics_run_one_training_iteration_time):
            with self.metrics.log_time((TIMERS, TRAINING_ITERATION_TIMER)):
                # In case we are training (in a thread) parallel to evaluation,
                # we may have to re-enable eager mode here (gets disabled in the
                # thread).
                if self.config.get("framework") == "tf2" and not tf.executing_eagerly():
                    tf1.enable_eager_execution()

                has_run_once = False
                # Create a step context ...
                with TrainIterCtx(algo=self) as train_iter_ctx:
                    # .. so we can query it whether we should stop the iteration loop (e.g.
                    # when we have reached `min_time_s_per_iteration`).
                    while not train_iter_ctx.should_stop(has_run_once):
                        # Before training step, try to bring failed workers back.
                        with self.metrics.log_time((TIMERS, RESTORE_ENV_RUNNERS_TIMER)):
                            restored = self.restore_env_runners(self.env_runner_group)
                            # Fire the callback for re-created EnvRunners.
                            if restored:
                                self._make_on_env_runners_recreated_callbacks(
                                    config=self.config,
                                    env_runner_group=self.env_runner_group,
                                    restored_env_runner_indices=restored,
                                )

                        # Try to train one step.
                        with self.metrics.log_time((TIMERS, TRAINING_STEP_TIMER)):
                            with TimerAndPrometheusLogger(
                                self._metrics_training_step_time
                            ):
                                training_step_return_value = self.training_step()
                            has_run_once = True

                        # On the new API stack, results should NOT be returned anymore as
                        # a dict, but purely logged through the `MetricsLogger` API. This
                        # way, we make sure to never miss a single stats/counter/timer
                        # when calling `self.training_step()` more than once within the same
                        # iteration.
                        if training_step_return_value is not None:
                            raise ValueError(
                                "`Algorithm.training_step()` should NOT return a result "
                                "dict anymore on the new API stack! Instead, log all "
                                "results, timers, counters through the `self.metrics` "
                                "(MetricsLogger) instance of the Algorithm and return "
                                "None. The logged results are compiled automatically into "
                                "one single result dict per training iteration."
                            )

                        # TODO (sven): Resolve this metric through log_time's future
                        #  ability to compute throughput.
                        self.metrics.log_value(
                            NUM_TRAINING_STEP_CALLS_PER_ITERATION,
                            1,
                            reduce="sum",
                        )

            if self.config.num_aggregator_actors_per_learner:
                remote_aggregator_metrics = self._aggregator_actor_manager.foreach_actor_async_fetch_ready(
                    func=lambda actor: actor.get_metrics(),
                    tag="metrics",
                    timeout_seconds=0.0,
                    return_obj_refs=False,
                    # (Artur) TODO: In the future, we want to make aggregator actors fault tolerant and should make this configurable
                    ignore_ray_errors=False,
                )

                self.metrics.aggregate(
                    remote_aggregator_metrics,
                    key=AGGREGATOR_ACTOR_RESULTS,
                )

            # Only here (at the end of the iteration), compile the results into a single result dict.
            # Calling compile here reduces the metrics into single values and adds throughputs to the results where applicable.
            compiled_metrics = self.metrics.compile()

        return compiled_metrics, train_iter_ctx

    def _run_one_offline_evaluation(self):
        """Runs offline evaluation step via `self.offline_evaluate()` and handling runner
        failures.

        Returns:
            The results dict from the offline evaluation call.
        """
        # Restore crashed offline evaluation runners.
        if self.offline_eval_runner_group is not None:
            with self.metrics.log_time((TIMERS, RESTORE_OFFLINE_EVAL_RUNNERS_TIMER)):
                restored = self.restore_offline_eval_runners(
                    self.offline_eval_runner_group
                )
                if restored:
                    # Fire the callback for re-created offline evaluation runners.
                    make_callback(
                        "on_offline_eval_runners_recreated",
                        callbacks_objects=self.callbacks,
                        callbacks_functions=(
                            self.config.callbacks_on_offline_eval_runners_recreated
                        ),
                        kwargs=dict(
                            algorithm=self,
                            env_runner_group=self.offline_eval_runner_group,
                            env_runner_indices=restored,
                        ),
                    )

        # Run one offline evaluation and time it.
        with self.metrics.log_time((TIMERS, OFFLINE_EVALUATION_ITERATION_TIMER)):
            eval_results = self.evaluate_offline()

        # After evaluation, do a round of health check on remote eval runners to see if
        # any of the failed runners are back.
        if self.offline_eval_runner_group is not None:
            # Add number of healthy evaluation runners after this iteration.
            eval_results[
                "num_healthy_offline_eval_runners"
            ] = self.offline_eval_runner_group.num_healthy_remote_runners
            eval_results[
                "offline_runners_actor_manager_num_outstanding_async_reqs"
            ] = self.offline_eval_runner_group.num_in_flight_async_reqs
            eval_results[
                "num_remote_offline_eval_runners_restarts"
            ] = self.offline_eval_runner_group.num_remote_runner_restarts

        return {EVALUATION_RESULTS: eval_results}

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
        with TimerAndPrometheusLogger(self._metrics_run_one_evaluation_time):
            if self.eval_env_runner_group is not None:
                if self.config.enable_env_runner_and_connector_v2:
                    with self.metrics.log_time(
                        (TIMERS, RESTORE_EVAL_ENV_RUNNERS_TIMER)
                    ):
                        restored = self.restore_env_runners(self.eval_env_runner_group)
                else:
                    with self._timers["restore_eval_workers"]:
                        restored = self.restore_env_runners(self.eval_env_runner_group)
                # Fire the callback for re-created EnvRunners.
                if restored:
                    self._make_on_env_runners_recreated_callbacks(
                        config=self.evaluation_config,
                        env_runner_group=self.eval_env_runner_group,
                        restored_env_runner_indices=restored,
                    )

            # Run `self.evaluate()` only once per training iteration.
            if self.config.enable_env_runner_and_connector_v2:
                with self.metrics.log_time((TIMERS, EVALUATION_ITERATION_TIMER)):
                    eval_results = self.evaluate(
                        parallel_train_future=parallel_train_future
                    )
            else:
                with self._timers[EVALUATION_ITERATION_TIMER]:
                    eval_results = self.evaluate(
                        parallel_train_future=parallel_train_future
                    )
                self._timers[EVALUATION_ITERATION_TIMER].push_units_processed(
                    self._counters[NUM_ENV_STEPS_SAMPLED_FOR_EVALUATION_THIS_ITER]
                )

            # After evaluation, do a round of health check on remote eval workers to see if
            # any of the failed workers are back.
            if self.eval_env_runner_group is not None:
                # Add number of healthy evaluation workers after this iteration.
                eval_results[
                    "num_healthy_workers"
                ] = self.eval_env_runner_group.num_healthy_remote_workers()
                eval_results[
                    "actor_manager_num_outstanding_async_reqs"
                ] = self.eval_env_runner_group.num_in_flight_async_reqs()
                eval_results[
                    "num_remote_worker_restarts"
                ] = self.eval_env_runner_group.num_remote_worker_restarts()

        return {EVALUATION_RESULTS: eval_results}

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

            if self.config.enable_env_runner_and_connector_v2:
                parallel_train_future = executor.submit(
                    lambda: self._run_one_training_iteration()
                )
            else:
                parallel_train_future = executor.submit(
                    lambda: self._run_one_training_iteration_old_api_stack()
                )

            # Pass the train_future into `self._run_one_evaluation()` to allow it
            # to run exactly as long as the training iteration takes in case
            # evaluation_duration=auto.
            evaluation_results = self._run_one_evaluation(
                parallel_train_future=parallel_train_future
            )
            # Collect the training results from the future.
            train_results, train_iter_ctx = parallel_train_future.result()

        return train_results, evaluation_results, train_iter_ctx

    def _run_offline_evaluation_old_api_stack(self):
        """Runs offline evaluation via `OfflineEvaluator.estimate_on_dataset()` API.

        This method will be used when `evaluation_dataset` is provided.
        Note: This will only work if the policy is a single agent policy.

        Returns:
            The results dict from the offline evaluation call.
        """
        assert len(self.env_runner_group.local_env_runner.policy_map) == 1

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
    def _should_create_evaluation_env_runners(cls, eval_config: "AlgorithmConfig"):
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

    # TODO (simon, sven): Flexibilize the different env/offline components and move
    # away from the currently hard-coded: (1) eval `EnvRunnerGroup`, (2) OfflineData
    # and (3) `OfflineEvaluationRunnerGroup`.
    @classmethod
    def _should_create_offline_evaluation_runners(cls, eval_config: "AlgorithmConfig"):
        """Determines whether we need to create offline evaluation workers."""

        return (
            eval_config.offline_evaluation_interval is not None
            or eval_config.num_offline_eval_runners > 0
        )

    def _compile_iteration_results(self, *, train_results, eval_results):
        with TimerAndPrometheusLogger(self._metrics_compile_iteration_results_time):
            # Error if users still use `self._timers`.
            if self._timers:
                raise ValueError(
                    "`Algorithm._timers` is no longer supported on the new API stack! "
                    "Instead, use `Algorithm.metrics.log_time("
                    "[some key (str) or nested key sequence (tuple)])`, e.g. inside your "
                    "custom `training_step()` method, do: "
                    "`with self.metrics.log_time(('timers', 'my_block_to_be_timed')): ...`"
                )

            # Return dict (shallow copy of `train_results`).
            results: ResultDict = train_results.copy()
            # Backward compatibility `NUM_ENV_STEPS_SAMPLED_LIFETIME` is now:
            # `ENV_RUNNER_RESULTS/NUM_ENV_STEPS_SAMPLED_LIFETIME`.
            results[NUM_ENV_STEPS_SAMPLED_LIFETIME] = results.get(
                ENV_RUNNER_RESULTS, {}
            ).get(NUM_ENV_STEPS_SAMPLED_LIFETIME, 0)

            # Evaluation results.
            if eval_results:
                assert (
                    isinstance(eval_results, dict)
                    and len(eval_results) == 1
                    and EVALUATION_RESULTS in eval_results
                )
                results.update(eval_results)

            # EnvRunner actors fault tolerance stats.
            if self.env_runner_group:
                results[FAULT_TOLERANCE_STATS] = {
                    "num_healthy_workers": (
                        self.env_runner_group.num_healthy_remote_workers()
                    ),
                    "num_remote_worker_restarts": (
                        self.env_runner_group.num_remote_worker_restarts()
                    ),
                }
                results["env_runner_group"] = {
                    "actor_manager_num_outstanding_async_reqs": (
                        self.env_runner_group.num_in_flight_async_reqs()
                    ),
                }

        return results

    def _make_on_env_runners_recreated_callbacks(
        self,
        *,
        config,
        env_runner_group,
        restored_env_runner_indices,
    ):
        make_callback(
            "on_env_runners_recreated",
            callbacks_objects=self.callbacks,
            callbacks_functions=(config.callbacks_on_env_runners_recreated),
            kwargs=dict(
                algorithm=self,
                env_runner_group=env_runner_group,
                env_runner_indices=restored_env_runner_indices,
                is_evaluation=config.in_evaluation,
            ),
        )
        # TODO (sven): Deprecate this call.
        make_callback(
            "on_workers_recreated",
            callbacks_objects=self.callbacks,
            kwargs=dict(
                algorithm=self,
                worker_set=env_runner_group,
                worker_ids=restored_env_runner_indices,
                is_evaluation=config.in_evaluation,
            ),
        )

    def __repr__(self):
        if self.config.enable_rl_module_and_learner:
            return (
                f"{type(self).__name__}("
                f"env={self.config.env}; env-runners={self.config.num_env_runners}; "
                f"learners={self.config.num_learners}; "
                f"multi-agent={self.config.is_multi_agent}"
                f")"
            )
        else:
            return type(self).__name__

    @property
    def env_runner(self):
        """The local EnvRunner instance within the algo's EnvRunnerGroup."""
        if self.env_runner_group:
            return self.env_runner_group.local_env_runner
        return None

    @property
    def eval_env_runner(self):
        """The local EnvRunner instance within the algo's evaluation EnvRunnerGroup."""
        if self.eval_env_runner_group:
            return self.eval_env_runner_group.local_env_runner
        return None

    def _record_usage(self, config):
        """Record the framework and algorithm used.

        Args:
            config: Algorithm config dict.
        """
        record_extra_usage_tag(TagKey.RLLIB_FRAMEWORK, config["framework"])
        record_extra_usage_tag(TagKey.RLLIB_NUM_WORKERS, str(config["num_env_runners"]))
        alg = self.__class__.__name__
        # We do not want to collect user defined algorithm names.
        if alg not in ALL_ALGORITHMS:
            alg = "USER_DEFINED"
        record_extra_usage_tag(TagKey.RLLIB_ALGORITHM, alg)

    @OldAPIStack
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

    @OldAPIStack
    def __getstate__(self) -> Dict:
        """Returns current state of Algorithm, sufficient to restore it from scratch.

        Returns:
            The current state dict of this Algorithm, which can be used to sufficiently
            restore the algorithm from scratch without any other information.
        """
        if self.config.enable_env_runner_and_connector_v2:
            raise RuntimeError(
                "Algorithm.__getstate__() not supported anymore on the new API stack! "
                "Use Algorithm.get_state() instead."
            )

        # Add config to state so complete Algorithm can be reproduced w/o it.
        state = {
            "algorithm_class": type(self),
            "config": self.config.get_state(),
        }

        if hasattr(self, "env_runner_group"):
            state["worker"] = self.env_runner_group.local_env_runner.get_state()

        # Also store eval `policy_mapping_fn` (in case it's different from main
        # one). Note, the new `EnvRunner API` has no policy mapping function.
        if (
            hasattr(self, "eval_env_runner_group")
            and self.eval_env_runner_group is not None
        ):
            state["eval_policy_mapping_fn"] = self.eval_env_runner.policy_mapping_fn

        # Save counters.
        state["counters"] = self._counters

        # TODO: Experimental functionality: Store contents of replay buffer
        #  to checkpoint, only if user has configured this.
        if self.local_replay_buffer is not None and self.config.get(
            "store_buffer_in_checkpoints"
        ):
            state["local_replay_buffer"] = self.local_replay_buffer.get_state()

        # Save current `training_iteration`.
        state[TRAINING_ITERATION] = self.training_iteration

        return state

    @OldAPIStack
    def __setstate__(self, state) -> None:
        """Sets the algorithm to the provided state.

        Args:
            state: The state dict to restore this Algorithm instance to. `state` may
                have been returned by a call to an Algorithm's `__getstate__()` method.
        """
        if self.config.enable_env_runner_and_connector_v2:
            raise RuntimeError(
                "Algorithm.__setstate__() not supported anymore on the new API stack! "
                "Use Algorithm.set_state() instead."
            )

        # Old API stack: The local worker stores its state (together with all the
        # Module information) in state['worker'].
        if hasattr(self, "env_runner_group") and "worker" in state and state["worker"]:
            self.env_runner.set_state(state["worker"])
            remote_state_ref = ray.put(state["worker"])
            self.env_runner_group.foreach_env_runner(
                lambda w: w.set_state(ray.get(remote_state_ref)),
                local_env_runner=False,
            )
            if self.eval_env_runner_group:
                # Avoid `state` being pickled into the remote function below.
                _eval_policy_mapping_fn = state.get("eval_policy_mapping_fn")

                def _setup_eval_worker(w):
                    w.set_state(ray.get(remote_state_ref))
                    # Override `policy_mapping_fn` as it might be different for eval
                    # workers.
                    w.set_policy_mapping_fn(_eval_policy_mapping_fn)

                # If evaluation workers are used, also restore the policies
                # there in case they are used for evaluation purpose.
                self.eval_env_runner_group.foreach_env_runner(_setup_eval_worker)

        # Restore replay buffer data.
        if self.local_replay_buffer is not None:
            # TODO: Experimental functionality: Restore contents of replay
            #  buffer from checkpoint, only if user has configured this.
            if self.config.store_buffer_in_checkpoints:
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

        if "counters" in state:
            self._counters = state["counters"]

        if TRAINING_ITERATION in state:
            self._iteration = state[TRAINING_ITERATION]

    @OldAPIStack
    @staticmethod
    def _checkpoint_info_to_algorithm_state(
        checkpoint_info: dict,
        *,
        policy_ids: Optional[Collection[PolicyID]] = None,
        policy_mapping_fn: Optional[Callable[[AgentID, EpisodeID], PolicyID]] = None,
        policies_to_train: Optional[
            Union[
                Collection[PolicyID],
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
                data = f.read()
                state = msgpack.unpackb(data, raw=False)
            else:
                state = pickle.load(f)

        # Old API stack: Policies are in separate sub-dirs.
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

    @OldAPIStack
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

        # Add parameters, if necessary.
        if "EpisodeReplayBuffer" in config["replay_buffer_config"]["type"]:
            # TODO (simon): Subclassing needs a proper class and therefore
            # we need at this moment the string checking. Because we add
            # this keyword argument the old stack ReplayBuffer constructors
            # will exit with an error b/c tje keyword argument is unknown to them.
            config["replay_buffer_config"][
                "metrics_num_episodes_for_smoothing"
            ] = self.config.metrics_num_episodes_for_smoothing

        return from_config(ReplayBuffer, config["replay_buffer_config"])

    @OldAPIStack
    def _run_one_training_iteration_old_api_stack(self):
        with self._timers[TRAINING_ITERATION_TIMER]:
            if self.config.get("framework") == "tf2" and not tf.executing_eagerly():
                tf1.enable_eager_execution()

            results = {}
            training_step_results = None
            with TrainIterCtx(algo=self) as train_iter_ctx:
                while not train_iter_ctx.should_stop(training_step_results):
                    with self._timers["restore_workers"]:
                        restored = self.restore_env_runners(self.env_runner_group)
                        # Fire the callback for re-created EnvRunners.
                        if restored:
                            self._make_on_env_runners_recreated_callbacks(
                                config=self.config,
                                env_runner_group=self.env_runner_group,
                                restored_env_runner_indices=restored,
                            )

                    with self._timers[TRAINING_STEP_TIMER]:
                        training_step_results = self.training_step()

                    if training_step_results:
                        results = training_step_results

        return results, train_iter_ctx

    @OldAPIStack
    def _compile_iteration_results_old_api_stack(
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
        results[ENV_RUNNER_RESULTS] = summarize_episodes(
            episodes_for_metrics,
            episodes_this_iter,
            self.config.keep_per_episode_custom_metrics,
        )

        results[
            "num_healthy_workers"
        ] = self.env_runner_group.num_healthy_remote_workers()
        results[
            "actor_manager_num_outstanding_async_reqs"
        ] = self.env_runner_group.num_in_flight_async_reqs()
        results[
            "num_remote_worker_restarts"
        ] = self.env_runner_group.num_remote_worker_restarts()

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

    @OldAPIStack
    @Deprecated(
        help="`Algorithm.compute_single_action` should no longer be used. Get the "
        "RLModule instance through `Algorithm.get_module([module ID])`, then compute "
        "actions through `RLModule.forward_inference({'obs': [obs batch]})`.",
        error=False,
    )
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
        episode=None,
        unsquash_action: Optional[bool] = None,
        clip_action: Optional[bool] = None,
    ) -> Union[
        TensorStructType,
        Tuple[TensorStructType, List[TensorType], Dict[str, TensorType]],
    ]:
        if unsquash_action is None:
            unsquash_action = self.config.normalize_actions
        elif clip_action is None:
            clip_action = self.config.clip_actions

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

        policy = self.get_policy(policy_id)
        if policy is None:
            raise KeyError(
                f"PolicyID '{policy_id}' not found in PolicyMap of the "
                f"Algorithm's local worker!"
            )
        pp = policy.agent_connectors[ObsPreprocessorConnector]

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
                pp.in_eval()
                if observation is not None:
                    _input_dict = {Columns.OBS: observation}
                elif input_dict is not None:
                    _input_dict = {Columns.OBS: input_dict[Columns.OBS]}
                else:
                    raise ValueError(
                        "Either observation or input_dict must be provided."
                    )

                acd = AgentConnectorDataType("0", "0", _input_dict)
                pp.reset(env_id="0")
                ac_o = pp([acd])[0]
                observation = ac_o.data[Columns.OBS]

        if input_dict is not None:
            input_dict[Columns.OBS] = observation
            action, state, extra = policy.compute_single_action(
                input_dict=input_dict,
                explore=explore,
                timestep=timestep,
                episode=episode,
            )
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

        if unsquash_action:
            action = space_utils.unsquash_action(action, policy.action_space_struct)
        elif clip_action:
            action = space_utils.clip_action(action, policy.action_space_struct)

        if state or full_fetch:
            return action, state, extra
        else:
            return action

    @OldAPIStack
    @Deprecated(
        help="`Algorithm.compute_actions` should no longer be used. Get the RLModule "
        "instance through `Algorithm.get_module([module ID])`, then compute actions "
        "through `RLModule.forward_inference({'obs': [obs batch]})`.",
        error=False,
    )
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
        episodes=None,
        unsquash_actions: Optional[bool] = None,
        clip_actions: Optional[bool] = None,
    ):
        if unsquash_actions is None:
            unsquash_actions = self.config.normalize_actions
        elif clip_actions is None:
            clip_actions = self.config.clip_actions

        state_defined = state is not None
        policy = self.get_policy(policy_id)
        filtered_obs, filtered_state = [], []
        for agent_id, ob in observations.items():
            worker = self.env_runner_group.local_env_runner
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

        obs_batch = np.stack(filtered_obs)
        if state is None:
            state = []
        else:
            state = list(zip(*filtered_state))
            state = [np.stack(s) for s in state]

        input_dict = {Columns.OBS: obs_batch}

        if prev_action is not None:
            input_dict[SampleBatch.PREV_ACTIONS] = prev_action
        if prev_reward is not None:
            input_dict[SampleBatch.PREV_REWARDS] = prev_reward
        if info:
            input_dict[Columns.INFOS] = info
        for i, s in enumerate(state):
            input_dict[f"state_in_{i}"] = s

        actions, states, infos = policy.compute_actions_from_input_dict(
            input_dict=input_dict,
            explore=explore,
            timestep=timestep,
            episodes=episodes,
        )

        single_actions = space_utils.unbatch(actions)
        actions = {}
        for key, a in zip(observations, single_actions):
            if unsquash_actions:
                a = space_utils.unsquash_action(a, policy.action_space_struct)
            elif clip_actions:
                a = space_utils.clip_action(a, policy.action_space_struct)
            actions[key] = a

        unbatched_states = {}
        for idx, agent_id in enumerate(observations):
            unbatched_states[agent_id] = [s[idx] for s in states]

        if state_defined or full_fetch:
            return actions, unbatched_states, infos
        else:
            return actions

    @Deprecated(new="Algorithm.restore_env_runners", error=True)
    def restore_workers(self, *args, **kwargs):
        pass

    @Deprecated(
        new="Algorithm.env_runner_group",
        error=True,
    )
    @property
    def workers(self):
        return self.env_runner_group

    @Deprecated(
        new="Algorithm.eval_env_runner_group",
        error=True,
    )
    @property
    def evaluation_workers(self):
        return self.eval_env_runner_group


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
                (ENV_RUNNER_RESULTS, NUM_ENV_STEPS_SAMPLED_LIFETIME), default=0
            )
            self.init_env_steps_trained = self.algo.metrics.peek(
                (LEARNER_RESULTS, ALL_MODULES, NUM_ENV_STEPS_TRAINED_LIFETIME),
                default=0,
            )
            self.init_agent_steps_sampled = sum(
                self.algo.metrics.peek(
                    (ENV_RUNNER_RESULTS, NUM_AGENT_STEPS_SAMPLED_LIFETIME), default={}
                ).values()
            )
            self.init_agent_steps_trained = sum(
                self.algo.metrics.peek(
                    (LEARNER_RESULTS, NUM_AGENT_STEPS_TRAINED_LIFETIME), default={}
                ).values()
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
        if results in [None, False]:
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
                            (ENV_RUNNER_RESULTS, NUM_AGENT_STEPS_SAMPLED_LIFETIME),
                            default={},
                        ).values()
                    )
                    - self.init_agent_steps_sampled
                )
                self.trained = (
                    sum(
                        self.algo.metrics.peek(
                            (LEARNER_RESULTS, NUM_AGENT_STEPS_TRAINED_LIFETIME),
                            default={},
                        ).values()
                    )
                    - self.init_agent_steps_trained
                )
            else:
                self.sampled = (
                    self.algo.metrics.peek(
                        (ENV_RUNNER_RESULTS, NUM_ENV_STEPS_SAMPLED_LIFETIME), default=0
                    )
                    - self.init_env_steps_sampled
                )
                self.trained = (
                    self.algo.metrics.peek(
                        (LEARNER_RESULTS, ALL_MODULES, NUM_ENV_STEPS_TRAINED_LIFETIME),
                        default=0,
                    )
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

import copy
from enum import Enum
import inspect
import logging
import math
import sys
from typing import (
    Any,
    Callable,
    Collection,
    Dict,
    List,
    Optional,
    Tuple,
    Type,
    TYPE_CHECKING,
    Union,
)

import gymnasium as gym
from packaging import version

import ray
from ray.rllib.algorithms.callbacks import DefaultCallbacks
from ray.rllib.core import DEFAULT_MODULE_ID
from ray.rllib.core.columns import Columns
from ray.rllib.core.rl_module import validate_module_id
from ray.rllib.core.rl_module.multi_rl_module import MultiRLModuleSpec
from ray.rllib.core.rl_module.rl_module import RLModuleSpec
from ray.rllib.env import INPUT_ENV_SPACES
from ray.rllib.env.env_context import EnvContext
from ray.rllib.env.multi_agent_env import MultiAgentEnv
from ray.rllib.env.wrappers.atari_wrappers import is_atari
from ray.rllib.evaluation.collectors.sample_collector import SampleCollector
from ray.rllib.evaluation.collectors.simple_list_collector import SimpleListCollector
from ray.rllib.models import MODEL_DEFAULTS
from ray.rllib.offline.input_reader import InputReader
from ray.rllib.offline.io_context import IOContext
from ray.rllib.policy.policy import Policy, PolicySpec
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID
from ray.rllib.utils import deep_update, merge_dicts
from ray.rllib.utils.annotations import (
    OldAPIStack,
    OverrideToImplementCustomLogic_CallToSuperRecommended,
)
from ray.rllib.utils.deprecation import (
    DEPRECATED_VALUE,
    Deprecated,
    deprecation_warning,
)
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.from_config import NotProvided, from_config
from ray.rllib.utils.schedules.scheduler import Scheduler
from ray.rllib.utils.serialization import (
    NOT_SERIALIZABLE,
    deserialize_type,
    serialize_type,
)
from ray.rllib.utils.torch_utils import TORCH_COMPILE_REQUIRED_VERSION
from ray.rllib.utils.typing import (
    AgentID,
    AlgorithmConfigDict,
    EnvConfigDict,
    EnvType,
    LearningRateOrSchedule,
    ModuleID,
    MultiAgentPolicyConfigDict,
    PartialAlgorithmConfigDict,
    PolicyID,
    ResultDict,
    RLModuleSpecType,
    SampleBatchType,
)
from ray.tune.logger import Logger
from ray.tune.registry import get_trainable_cls
from ray.tune.result import TRIAL_INFO
from ray.tune.tune import _Config

Space = gym.Space

"""TODO(jungong, sven): in "offline_data" we can potentially unify all input types
under input and input_config keys. E.g.
input: sample
input_config {
env: CartPole-v1
}
or:
input: json_reader
input_config {
path: /tmp/
}
or:
input: dataset
input_config {
format: parquet
path: /tmp/
}
"""


if TYPE_CHECKING:
    from ray.rllib.algorithms.algorithm import Algorithm
    from ray.rllib.connectors.connector_v2 import ConnectorV2
    from ray.rllib.core.learner import Learner
    from ray.rllib.core.learner.learner_group import LearnerGroup
    from ray.rllib.core.rl_module.rl_module import RLModule
    from ray.rllib.evaluation.episode import Episode as OldEpisode

logger = logging.getLogger(__name__)


def _check_rl_module_spec(module_spec: RLModuleSpecType) -> None:
    if not isinstance(module_spec, (RLModuleSpec, MultiRLModuleSpec)):
        raise ValueError(
            "rl_module_spec must be an instance of "
            "RLModuleSpec or MultiRLModuleSpec."
            f"Got {type(module_spec)} instead."
        )


class AlgorithmConfig(_Config):
    """A RLlib AlgorithmConfig builds an RLlib Algorithm from a given configuration.

    .. testcode::

        from ray.rllib.algorithms.ppo import PPOConfig
        from ray.rllib.algorithms.callbacks import MemoryTrackingCallbacks
        # Construct a generic config object, specifying values within different
        # sub-categories, e.g. "training".
        config = (PPOConfig().training(gamma=0.9, lr=0.01)
                .environment(env="CartPole-v1")
                .resources(num_gpus=0)
                .env_runners(num_env_runners=0)
                .callbacks(MemoryTrackingCallbacks)
            )
        # A config object can be used to construct the respective Algorithm.
        rllib_algo = config.build()

    .. testcode::

        from ray.rllib.algorithms.ppo import PPOConfig
        from ray import tune
        # In combination with a tune.grid_search:
        config = PPOConfig()
        config.training(lr=tune.grid_search([0.01, 0.001]))
        # Use `to_dict()` method to get the legacy plain python config dict
        # for usage with `tune.Tuner().fit()`.
        tune.Tuner("PPO", param_space=config.to_dict())
    """

    @staticmethod
    def DEFAULT_AGENT_TO_MODULE_MAPPING_FN(agent_id, episode):
        # The default agent ID to module ID mapping function to use in the multi-agent
        # case if None is provided.
        # Map any agent ID to "default_policy".
        return DEFAULT_MODULE_ID

    # TODO (sven): Deprecate in new API stack.
    @staticmethod
    def DEFAULT_POLICY_MAPPING_FN(aid, episode, worker, **kwargs):
        # The default policy mapping function to use if None provided.
        # Map any agent ID to "default_policy".
        return DEFAULT_POLICY_ID

    @classmethod
    def from_dict(cls, config_dict: dict) -> "AlgorithmConfig":
        """Creates an AlgorithmConfig from a legacy python config dict.

        .. testcode::

            from ray.rllib.algorithms.ppo.ppo import PPOConfig
            # pass a RLlib config dict
            ppo_config = PPOConfig.from_dict({})
            ppo = ppo_config.build(env="Pendulum-v1")

        Args:
            config_dict: The legacy formatted python config dict for some algorithm.

        Returns:
            A new AlgorithmConfig object that matches the given python config dict.
        """
        # Create a default config object of this class.
        config_obj = cls()
        # Remove `_is_frozen` flag from config dict in case the AlgorithmConfig that
        # the dict was derived from was already frozen (we don't want to copy the
        # frozenness).
        config_dict.pop("_is_frozen", None)
        config_obj.update_from_dict(config_dict)
        return config_obj

    @classmethod
    def overrides(cls, **kwargs):
        """Generates and validates a set of config key/value pairs (passed via kwargs).

        Validation whether given config keys are valid is done immediately upon
        construction (by comparing against the properties of a default AlgorithmConfig
        object of this class).
        Allows combination with a full AlgorithmConfig object to yield a new
        AlgorithmConfig object.

        Used anywhere, we would like to enable the user to only define a few config
        settings that would change with respect to some main config, e.g. in multi-agent
        setups and evaluation configs.

        .. testcode::

            from ray.rllib.algorithms.ppo import PPOConfig
            from ray.rllib.policy.policy import PolicySpec
            config = (
                PPOConfig()
                .multi_agent(
                    policies={
                        "pol0": PolicySpec(config=PPOConfig.overrides(lambda_=0.95))
                    },
                )
            )


        .. testcode::

            from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
            from ray.rllib.algorithms.ppo import PPOConfig
            config = (
                PPOConfig()
                .evaluation(
                    evaluation_num_env_runners=1,
                    evaluation_interval=1,
                    evaluation_config=AlgorithmConfig.overrides(explore=False),
                )
            )

        Returns:
            A dict mapping valid config property-names to values.

        Raises:
            KeyError: In case a non-existing property name (kwargs key) is being
                passed in. Valid property names are taken from a default
                AlgorithmConfig object of `cls`.
        """
        default_config = cls()
        config_overrides = {}
        for key, value in kwargs.items():
            if not hasattr(default_config, key):
                raise KeyError(
                    f"Invalid property name {key} for config class {cls.__name__}!"
                )
            # Allow things like "lambda" as well.
            key = cls._translate_special_keys(key, warn_deprecated=True)
            config_overrides[key] = value

        return config_overrides

    def __init__(self, algo_class: Optional[type] = None):
        """Initializes an AlgorithmConfig instance.

        Args:
            algo_class: An optional Algorithm class that this config class belongs to.
                Used (if provided) to build a respective Algorithm instance from this
                config.
        """
        # Define all settings and their default values.

        # Define the default RLlib Algorithm class that this AlgorithmConfig will be
        # applied to.
        self.algo_class = algo_class

        # `self.python_environment()`
        self.extra_python_environs_for_driver = {}
        self.extra_python_environs_for_worker = {}

        # `self.resources()`
        self.placement_strategy = "PACK"
        self.num_gpus = 0  # @OldAPIStack
        self._fake_gpus = False  # @OldAPIStack
        self.num_cpus_for_main_process = 1

        # `self.framework()`
        self.framework_str = "torch"
        self.eager_tracing = True
        self.eager_max_retraces = 20
        self.tf_session_args = {
            # note: overridden by `local_tf_session_args`
            "intra_op_parallelism_threads": 2,
            "inter_op_parallelism_threads": 2,
            "gpu_options": {
                "allow_growth": True,
            },
            "log_device_placement": False,
            "device_count": {"CPU": 1},
            # Required by multi-GPU (num_gpus > 1).
            "allow_soft_placement": True,
        }
        self.local_tf_session_args = {
            # Allow a higher level of parallelism by default, but not unlimited
            # since that can cause crashes with many concurrent drivers.
            "intra_op_parallelism_threads": 8,
            "inter_op_parallelism_threads": 8,
        }
        # Torch compile settings
        self.torch_compile_learner = False
        self.torch_compile_learner_what_to_compile = (
            TorchCompileWhatToCompile.FORWARD_TRAIN
        )
        # AOT Eager is a dummy backend and will not result in speedups
        self.torch_compile_learner_dynamo_backend = (
            "aot_eager" if sys.platform == "darwin" else "inductor"
        )
        self.torch_compile_learner_dynamo_mode = None
        self.torch_compile_worker = False
        # AOT Eager is a dummy backend and will not result in speedups
        self.torch_compile_worker_dynamo_backend = (
            "aot_eager" if sys.platform == "darwin" else "onnxrt"
        )
        self.torch_compile_worker_dynamo_mode = None
        # Default kwargs for `torch.nn.parallel.DistributedDataParallel`.
        self.torch_ddp_kwargs = {}
        # Default setting for skipping `nan` gradient updates.
        self.torch_skip_nan_gradients = False

        # `self.api_stack()`
        self.enable_rl_module_and_learner = False
        self.enable_env_runner_and_connector_v2 = False

        # `self.environment()`
        self.env = None
        self.env_config = {}
        self.observation_space = None
        self.action_space = None
        self.clip_rewards = None
        self.normalize_actions = True
        self.clip_actions = False
        self._is_atari = None
        self.disable_env_checking = False
        # Deprecated settings:
        self.env_task_fn = None
        self.render_env = False
        self.action_mask_key = "action_mask"

        # `self.env_runners()`
        self.env_runner_cls = None
        self.num_env_runners = 0
        self.num_envs_per_env_runner = 1
        self.num_cpus_per_env_runner = 1
        self.num_gpus_per_env_runner = 0
        self.custom_resources_per_env_runner = {}
        self.validate_env_runners_after_construction = True
        self.sample_timeout_s = 60.0
        self.create_env_on_local_worker = False
        self._env_to_module_connector = None
        self.add_default_connectors_to_env_to_module_pipeline = True
        self._module_to_env_connector = None
        self.add_default_connectors_to_module_to_env_pipeline = True
        self.episode_lookback_horizon = 1
        # TODO (sven): Rename into `sample_timesteps` (or `sample_duration`
        #  and `sample_duration_unit` (replacing batch_mode), like we do it
        #  in the evaluation config).
        self.rollout_fragment_length = 200
        # TODO (sven): Rename into `sample_mode`.
        self.batch_mode = "truncate_episodes"
        self.compress_observations = False
        # @OldAPIStack
        self.remote_worker_envs = False
        self.remote_env_batch_wait_ms = 0
        self.enable_tf1_exec_eagerly = False
        self.sample_collector = SimpleListCollector
        self.preprocessor_pref = "deepmind"
        self.observation_filter = "NoFilter"
        self.update_worker_filter_stats = True
        self.use_worker_filter_stats = True
        self.enable_connectors = True
        self.sampler_perf_stats_ema_coef = None

        # `self.learners()`
        self.num_learners = 0
        self.num_gpus_per_learner = 0
        self.num_cpus_per_learner = 1
        self.local_gpu_idx = 0

        # `self.training()`
        self.gamma = 0.99
        self.lr = 0.001
        self.grad_clip = None
        self.grad_clip_by = "global_norm"
        # Simple logic for now: If None, use `train_batch_size`.
        self.train_batch_size_per_learner = None
        self.train_batch_size = 32  # @OldAPIStack

        # These setting have been adopted from the original PPO batch settings:
        # num_sgd_iter, minibatch_size, and shuffle_sequences.
        self.num_epochs = 1
        self.minibatch_size = None
        self.shuffle_batch_per_epoch = False

        # TODO (sven): Unsolved problem with RLModules sometimes requiring settings from
        #  the main AlgorithmConfig. We should not require the user to provide those
        #  settings in both, the AlgorithmConfig (as property) AND the model config
        #  dict. We should generally move to a world, in which there exists an
        #  AlgorithmConfig that a) has-a user provided model config object and b)
        #  is given a chance to compile a final model config (dict or object) that is
        #  then passed into the RLModule/Catalog. This design would then match our
        #  "compilation" pattern, where we compile automatically those settings that
        #  should NOT be touched by the user.
        #  In case, an Algorithm already uses the above described pattern (and has
        #  `self.model` as a @property, ignore AttributeError (for trying to set this
        #  property).
        try:
            self.model = copy.deepcopy(MODEL_DEFAULTS)
        except AttributeError:
            pass

        self._learner_connector = None
        self.add_default_connectors_to_learner_pipeline = True
        self.learner_config_dict = {}
        self.optimizer = {}
        self.max_requests_in_flight_per_sampler_worker = 2
        self._learner_class = None

        # `self.callbacks()`
        self.callbacks_class = DefaultCallbacks

        # `self.explore()`
        self.explore = True
        # This is not compatible with RLModules, which have a method
        # `forward_exploration` to specify custom exploration behavior.
        self.exploration_config = {}

        # `self.multi_agent()`
        # TODO (sven): Prepare multi-agent setup for logging each agent's and each
        #  RLModule's steps taken thus far (and passing this information into the
        #  EnvRunner metrics and the RLModule's forward pass). Thereby, deprecate the
        #  `count_steps_by` config setting AND - at the same time - allow users to
        #  specify the batch size unit instead (agent- vs env steps).
        self.count_steps_by = "env_steps"
        # self.agent_to_module_mapping_fn = self.DEFAULT_AGENT_TO_MODULE_MAPPING_FN
        # Soon to be Deprecated.
        self.policies = {DEFAULT_POLICY_ID: PolicySpec()}
        self.policy_map_capacity = 100
        self.policy_mapping_fn = self.DEFAULT_POLICY_MAPPING_FN
        self.policies_to_train = None
        self.policy_states_are_swappable = False
        self.observation_fn = None

        # `self.offline_data()`
        self.input_ = "sampler"
        self.input_read_method = "read_parquet"
        self.input_read_method_kwargs = {}
        self.input_read_schema = {}
        self.input_read_episodes = False
        self.input_read_sample_batches = False
        self.input_filesystem = None
        self.input_filesystem_kwargs = {}
        self.input_compress_columns = [Columns.OBS, Columns.NEXT_OBS]
        self.input_spaces_jsonable = True
        self.materialize_data = False
        self.materialize_mapped_data = True
        self.map_batches_kwargs = {}
        self.iter_batches_kwargs = {}
        self.prelearner_class = None
        self.prelearner_buffer_class = None
        self.prelearner_buffer_kwargs = {}
        self.prelearner_module_synch_period = 10
        self.dataset_num_iters_per_learner = None
        self.input_config = {}
        self.actions_in_input_normalized = False
        self.postprocess_inputs = False
        self.shuffle_buffer_size = 0
        self.output = None
        self.output_config = {}
        self.output_compress_columns = [Columns.OBS, Columns.NEXT_OBS]
        self.output_max_file_size = 64 * 1024 * 1024
        self.output_max_rows_per_file = None
        self.output_write_method = "write_parquet"
        self.output_write_method_kwargs = {}
        self.output_filesystem = None
        self.output_filesystem_kwargs = {}
        self.output_write_episodes = True
        self.offline_sampling = False

        # `self.evaluation()`
        self.evaluation_interval = None
        self.evaluation_duration = 10
        self.evaluation_duration_unit = "episodes"
        self.evaluation_sample_timeout_s = 120.0
        self.evaluation_parallel_to_training = False
        self.evaluation_force_reset_envs_before_iteration = True
        self.evaluation_config = None
        self.off_policy_estimation_methods = {}
        self.ope_split_batch_by_episode = True
        self.evaluation_num_env_runners = 0
        self.custom_evaluation_function = None
        # TODO: Set this flag still in the config or - much better - in the
        #  RolloutWorker as a property.
        self.in_evaluation = False
        # TODO (sven): Deprecate this setting (it's not user-accessible right now any
        #  way). Replace by logic within `training_step` to merge and broadcast the
        #  EnvRunner (connector) states.
        self.sync_filters_on_rollout_workers_timeout_s = 10.0

        # `self.reporting()`
        self.keep_per_episode_custom_metrics = False
        self.metrics_episode_collection_timeout_s = 60.0
        self.metrics_num_episodes_for_smoothing = 100
        self.min_time_s_per_iteration = None
        self.min_train_timesteps_per_iteration = 0
        self.min_sample_timesteps_per_iteration = 0
        self.log_gradients = True

        # `self.checkpointing()`
        self.export_native_model_files = False
        self.checkpoint_trainable_policies_only = False

        # `self.debugging()`
        self.logger_creator = None
        self.logger_config = None
        self.log_level = "WARN"
        self.log_sys_usage = True
        self.fake_sampler = False
        self.seed = None
        # TODO (sven): Remove these settings again in the future. We only need them
        #  to debug a quite complex, production memory leak, possibly related to
        #  evaluation in parallel (when `training_step` is getting called inside a
        #  thread). It's also possible that the leak is not caused by RLlib itself,
        #  but by Ray core, but we need more data to narrow this down.
        self._run_training_always_in_thread = False
        self._evaluation_parallel_to_training_wo_thread = False

        # `self.fault_tolerance()`
        self.ignore_env_runner_failures = False
        self.recreate_failed_env_runners = False
        # By default, restart failed worker a thousand times.
        # This should be enough to handle normal transient failures.
        # This also prevents infinite number of restarts in case
        # the worker or env has a bug.
        self.max_num_env_runner_restarts = 1000
        # Small delay between worker restarts. In case rollout or
        # evaluation workers have remote dependencies, this delay can be
        # adjusted to make sure we don't flood them with re-connection
        # requests, and allow them enough time to recover.
        # This delay also gives Ray time to stream back error logging
        # and exceptions.
        self.delay_between_env_runner_restarts_s = 60.0
        self.restart_failed_sub_environments = False
        self.num_consecutive_env_runner_failures_tolerance = 100
        self.env_runner_health_probe_timeout_s = 30
        self.env_runner_restore_timeout_s = 1800

        # `self.rl_module()`
        self._model_config_dict = {}
        self._rl_module_spec = None
        # Helper to keep track of the original exploration config when dis-/enabling
        # rl modules.
        self.__prior_exploration_config = None
        # Module ID specific config overrides.
        self.algorithm_config_overrides_per_module = {}
        # Cached, actual AlgorithmConfig objects derived from
        # `self.algorithm_config_overrides_per_module`.
        self._per_module_overrides: Dict[ModuleID, "AlgorithmConfig"] = {}

        # `self.experimental()`
        self._torch_grad_scaler_class = None
        self._torch_lr_scheduler_classes = None
        self._tf_policy_handles_more_than_one_loss = False
        self._disable_preprocessor_api = False
        self._disable_action_flattening = False
        self._disable_initialize_loss_from_dummy_batch = False
        self._dont_auto_sync_env_runner_states = False

        # Has this config object been frozen (cannot alter its attributes anymore).
        self._is_frozen = False

        # TODO: Remove, once all deprecation_warning calls upon using these keys
        #  have been removed.
        # === Deprecated keys ===
        self.simple_optimizer = DEPRECATED_VALUE
        self.monitor = DEPRECATED_VALUE
        self.evaluation_num_episodes = DEPRECATED_VALUE
        self.metrics_smoothing_episodes = DEPRECATED_VALUE
        self.timesteps_per_iteration = DEPRECATED_VALUE
        self.min_iter_time_s = DEPRECATED_VALUE
        self.collect_metrics_timeout = DEPRECATED_VALUE
        self.min_time_s_per_reporting = DEPRECATED_VALUE
        self.min_train_timesteps_per_reporting = DEPRECATED_VALUE
        self.min_sample_timesteps_per_reporting = DEPRECATED_VALUE
        self.input_evaluation = DEPRECATED_VALUE
        self.policy_map_cache = DEPRECATED_VALUE
        self.worker_cls = DEPRECATED_VALUE
        self.synchronize_filters = DEPRECATED_VALUE
        self.enable_async_evaluation = DEPRECATED_VALUE
        self.custom_async_evaluation_function = DEPRECATED_VALUE
        self._enable_rl_module_api = DEPRECATED_VALUE
        self.auto_wrap_old_gym_envs = DEPRECATED_VALUE
        self.always_attach_evaluation_results = DEPRECATED_VALUE

        # The following values have moved because of the new ReplayBuffer API
        self.buffer_size = DEPRECATED_VALUE
        self.prioritized_replay = DEPRECATED_VALUE
        self.learning_starts = DEPRECATED_VALUE
        self.replay_batch_size = DEPRECATED_VALUE
        # -1 = DEPRECATED_VALUE is a valid value for replay_sequence_length
        self.replay_sequence_length = None
        self.replay_mode = DEPRECATED_VALUE
        self.prioritized_replay_alpha = DEPRECATED_VALUE
        self.prioritized_replay_beta = DEPRECATED_VALUE
        self.prioritized_replay_eps = DEPRECATED_VALUE
        self.min_time_s_per_reporting = DEPRECATED_VALUE
        self.min_train_timesteps_per_reporting = DEPRECATED_VALUE
        self.min_sample_timesteps_per_reporting = DEPRECATED_VALUE
        self._disable_execution_plan_api = DEPRECATED_VALUE

    def to_dict(self) -> AlgorithmConfigDict:
        """Converts all settings into a legacy config dict for backward compatibility.

        Returns:
            A complete AlgorithmConfigDict, usable in backward-compatible Tune/RLlib
            use cases.
        """
        config = copy.deepcopy(vars(self))
        config.pop("algo_class")
        config.pop("_is_frozen")

        # Worst naming convention ever: NEVER EVER use reserved key-words...
        if "lambda_" in config:
            assert hasattr(self, "lambda_")
            config["lambda"] = getattr(self, "lambda_")
            config.pop("lambda_")
        if "input_" in config:
            assert hasattr(self, "input_")
            config["input"] = getattr(self, "input_")
            config.pop("input_")

        # Convert `policies` (PolicySpecs?) into dict.
        # Convert policies dict such that each policy ID maps to a old-style.
        # 4-tuple: class, obs-, and action space, config.
        if "policies" in config and isinstance(config["policies"], dict):
            policies_dict = {}
            for policy_id, policy_spec in config.pop("policies").items():
                if isinstance(policy_spec, PolicySpec):
                    policies_dict[policy_id] = policy_spec.get_state()
                else:
                    policies_dict[policy_id] = policy_spec
            config["policies"] = policies_dict

        # Switch out deprecated vs new config keys.
        config["callbacks"] = config.pop("callbacks_class", DefaultCallbacks)
        config["create_env_on_driver"] = config.pop("create_env_on_local_worker", 1)
        config["custom_eval_function"] = config.pop("custom_evaluation_function", None)
        config["framework"] = config.pop("framework_str", None)

        # Simplify: Remove all deprecated keys that have as value `DEPRECATED_VALUE`.
        # These would be useless in the returned dict anyways.
        for dep_k in [
            "monitor",
            "evaluation_num_episodes",
            "metrics_smoothing_episodes",
            "timesteps_per_iteration",
            "min_iter_time_s",
            "collect_metrics_timeout",
            "buffer_size",
            "prioritized_replay",
            "learning_starts",
            "replay_batch_size",
            "replay_mode",
            "prioritized_replay_alpha",
            "prioritized_replay_beta",
            "prioritized_replay_eps",
            "min_time_s_per_reporting",
            "min_train_timesteps_per_reporting",
            "min_sample_timesteps_per_reporting",
            "input_evaluation",
            "_enable_new_api_stack",
        ]:
            if config.get(dep_k) == DEPRECATED_VALUE:
                config.pop(dep_k, None)

        return config

    def update_from_dict(
        self,
        config_dict: PartialAlgorithmConfigDict,
    ) -> "AlgorithmConfig":
        """Modifies this AlgorithmConfig via the provided python config dict.

        Warns if `config_dict` contains deprecated keys.
        Silently sets even properties of `self` that do NOT exist. This way, this method
        may be used to configure custom Policies which do not have their own specific
        AlgorithmConfig classes, e.g.
        `ray.rllib.examples.policy.random_policy::RandomPolicy`.

        Args:
            config_dict: The old-style python config dict (PartialAlgorithmConfigDict)
                to use for overriding some properties defined in there.

        Returns:
            This updated AlgorithmConfig object.
        """
        eval_call = {}

        # We deal with this special key before all others because it may influence
        # stuff like "exploration_config".
        # Namely, we want to re-instantiate the exploration config this config had
        # inside `self.experimental()` before potentially overwriting it in the
        # following.
        enable_rl_module_and_learner = config_dict.get(
            "_enable_new_api_stack",
            config_dict.get("enable_rl_module_and_learner"),
        )
        if enable_rl_module_and_learner:
            self.api_stack(enable_rl_module_and_learner=enable_rl_module_and_learner)

        # Modify our properties one by one.
        for key, value in config_dict.items():
            key = self._translate_special_keys(key, warn_deprecated=False)

            # Ray Tune saves additional data under this magic keyword.
            # This should not get treated as AlgorithmConfig field.
            if key == TRIAL_INFO:
                continue

            if key in ["_enable_new_api_stack", "enable_rl_module_and_learner"]:
                # We've dealt with this above.
                continue
            # Set our multi-agent settings.
            elif key == "multiagent":
                kwargs = {
                    k: value[k]
                    for k in [
                        "policies",
                        "policy_map_capacity",
                        "policy_mapping_fn",
                        "policies_to_train",
                        "policy_states_are_swappable",
                        "observation_fn",
                        "count_steps_by",
                    ]
                    if k in value
                }
                self.multi_agent(**kwargs)
            # Some keys specify config sub-dicts and therefore should go through the
            # correct methods to properly `.update()` those from given config dict
            # (to not lose any sub-keys).
            elif key == "callbacks_class" and value != NOT_SERIALIZABLE:
                # For backward compatibility reasons, only resolve possible
                # classpath if value is a str type.
                if isinstance(value, str):
                    value = deserialize_type(value, error=True)
                self.callbacks(callbacks_class=value)
            elif key == "env_config":
                self.environment(env_config=value)
            elif key.startswith("evaluation_"):
                eval_call[key] = value
            elif key == "exploration_config":
                if enable_rl_module_and_learner:
                    self.exploration_config = value
                    continue
                if isinstance(value, dict) and "type" in value:
                    value["type"] = deserialize_type(value["type"])
                self.env_runners(exploration_config=value)
            elif key == "model":
                # Resolve possible classpath.
                if isinstance(value, dict) and value.get("custom_model"):
                    value["custom_model"] = deserialize_type(value["custom_model"])
                self.training(**{key: value})
            elif key == "optimizer":
                self.training(**{key: value})
            elif key == "replay_buffer_config":
                if isinstance(value, dict) and "type" in value:
                    value["type"] = deserialize_type(value["type"])
                self.training(**{key: value})
            elif key == "sample_collector":
                # Resolve possible classpath.
                value = deserialize_type(value)
                self.env_runners(sample_collector=value)
            # Set the property named `key` to `value`.
            else:
                setattr(self, key, value)

        self.evaluation(**eval_call)

        return self

    def get_state(self) -> Dict[str, Any]:
        """Returns a dict state that can be pickled.

        Returns:
            A dictionary containing all attributes of the instance.
        """

        state = self.__dict__.copy()
        state["class"] = type(self)
        state.pop("algo_class")
        state.pop("_is_frozen")
        state = {k: v for k, v in state.items() if v != DEPRECATED_VALUE}

        # Convert `policies` (PolicySpecs?) into dict.
        # Convert policies dict such that each policy ID maps to a old-style.
        # 4-tuple: class, obs-, and action space, config.
        # TODO (simon, sven): Remove when deprecating old stack.
        if "policies" in state and isinstance(state["policies"], dict):
            policies_dict = {}
            for policy_id, policy_spec in state.pop("policies").items():
                if isinstance(policy_spec, PolicySpec):
                    policies_dict[policy_id] = policy_spec.get_state()
                else:
                    policies_dict[policy_id] = policy_spec
            state["policies"] = policies_dict

        # state = self._serialize_dict(state)

        return state

    @classmethod
    def from_state(cls, state: Dict[str, Any]) -> "AlgorithmConfig":
        """Returns an instance constructed from the state.

        Args:
            cls: An `AlgorithmConfig` class.
            state: A dictionary containing the state of an `AlgorithmConfig`.
                See `AlgorithmConfig.get_state` for creating a state.

        Returns:
            An `AlgorithmConfig` instance with attributes from the `state`.
        """

        ctor = state["class"]
        config = ctor()

        config.__dict__.update(state)

        return config

    # TODO(sven): We might want to have a `deserialize` method as well. Right now,
    #  simply using the from_dict() API works in this same (deserializing) manner,
    #  whether the dict used is actually code-free (already serialized) or not
    #  (i.e. a classic RLlib config dict with e.g. "callbacks" key still pointing to
    #  a class).
    def serialize(self) -> Dict[str, Any]:
        """Returns a mapping from str to JSON'able values representing this config.

        The resulting values will not have any code in them.
        Classes (such as `callbacks_class`) will be converted to their full
        classpath, e.g. `ray.rllib.algorithms.callbacks.DefaultCallbacks`.
        Actual code such as lambda functions will be written as their source
        code (str) plus any closure information for properly restoring the
        code inside the AlgorithmConfig object made from the returned dict data.
        Dataclass objects get converted to dicts.

        Returns:
            A dict mapping from str to JSON'able values.
        """
        config = self.to_dict()
        return self._serialize_dict(config)

    def copy(self, copy_frozen: Optional[bool] = None) -> "AlgorithmConfig":
        """Creates a deep copy of this config and (un)freezes if necessary.

        Args:
            copy_frozen: Whether the created deep copy will be frozen or not. If None,
                keep the same frozen status that `self` currently has.

        Returns:
            A deep copy of `self` that is (un)frozen.
        """
        cp = copy.deepcopy(self)
        if copy_frozen is True:
            cp.freeze()
        elif copy_frozen is False:
            cp._is_frozen = False
            if isinstance(cp.evaluation_config, AlgorithmConfig):
                cp.evaluation_config._is_frozen = False
        return cp

    def freeze(self) -> None:
        """Freezes this config object, such that no attributes can be set anymore.

        Algorithms should use this method to make sure that their config objects
        remain read-only after this.
        """
        if self._is_frozen:
            return
        self._is_frozen = True

        # Also freeze underlying eval config, if applicable.
        if isinstance(self.evaluation_config, AlgorithmConfig):
            self.evaluation_config.freeze()

        # TODO: Flip out all set/dict/list values into frozen versions
        #  of themselves? This way, users won't even be able to alter those values
        #  directly anymore.

    @OverrideToImplementCustomLogic_CallToSuperRecommended
    def validate(self) -> None:
        """Validates all values in this config."""

        # Check framework specific settings.
        self._validate_framework_settings()
        # Check resources specific settings.
        self._validate_resources_settings()
        # Check multi-agent specific settings.
        self._validate_multi_agent_settings()
        # Check input specific settings.
        self._validate_input_settings()
        # Check evaluation specific settings.
        self._validate_evaluation_settings()
        # Check offline specific settings (new API stack).
        self._validate_offline_settings()

        # Check new API stack specific settings.
        self._validate_new_api_stack_settings()

        # Check to-be-deprecated settings (however that are still in use).
        self._validate_to_be_deprecated_settings()

    def build(
        self,
        env: Optional[Union[str, EnvType]] = None,
        logger_creator: Optional[Callable[[], Logger]] = None,
        use_copy: bool = True,
    ) -> "Algorithm":
        """Builds an Algorithm from this AlgorithmConfig (or a copy thereof).

        Args:
            env: Name of the environment to use (e.g. a gym-registered str),
                a full class path (e.g.
                "ray.rllib.examples.envs.classes.random_env.RandomEnv"), or an Env
                class directly. Note that this arg can also be specified via
                the "env" key in `config`.
            logger_creator: Callable that creates a ray.tune.Logger
                object. If unspecified, a default logger is created.
            use_copy: Whether to deepcopy `self` and pass the copy to the Algorithm
                (instead of `self`) as config. This is useful in case you would like to
                recycle the same AlgorithmConfig over and over, e.g. in a test case, in
                which we loop over different DL-frameworks.

        Returns:
            A ray.rllib.algorithms.algorithm.Algorithm object.
        """
        if env is not None:
            self.env = env
            if self.evaluation_config is not None:
                self.evaluation_config["env"] = env
        if logger_creator is not None:
            self.logger_creator = logger_creator

        algo_class = self.algo_class
        if isinstance(self.algo_class, str):
            algo_class = get_trainable_cls(self.algo_class)

        return algo_class(
            config=self if not use_copy else copy.deepcopy(self),
            logger_creator=self.logger_creator,
        )

    def build_env_to_module_connector(self, env):
        from ray.rllib.connectors.env_to_module import (
            AddObservationsFromEpisodesToBatch,
            AddStatesFromEpisodesToBatch,
            AgentToModuleMapping,
            BatchIndividualItems,
            EnvToModulePipeline,
            NumpyToTensor,
        )

        custom_connectors = []
        # Create an env-to-module connector pipeline (including RLlib's default
        # env->module connector piece) and return it.
        if self._env_to_module_connector is not None:
            val_ = self._env_to_module_connector(env)

            from ray.rllib.connectors.connector_v2 import ConnectorV2

            # ConnectorV2 (piece or pipeline).
            if isinstance(val_, ConnectorV2):
                custom_connectors = [val_]
            # Sequence of individual ConnectorV2 pieces.
            elif isinstance(val_, (list, tuple)):
                custom_connectors = list(val_)
            # Unsupported return value.
            else:
                raise ValueError(
                    "`AlgorithmConfig.env_runners(env_to_module_connector=..)` must "
                    "return a ConnectorV2 object or a list thereof (to be added to a "
                    f"pipeline)! Your function returned {val_}."
                )

        pipeline = EnvToModulePipeline(
            connectors=custom_connectors,
            input_observation_space=getattr(
                env, "single_observation_space", env.observation_space
            ),
            input_action_space=getattr(env, "single_action_space", env.action_space),
        )

        if self.add_default_connectors_to_env_to_module_pipeline:
            # Append OBS handling.
            pipeline.append(AddObservationsFromEpisodesToBatch())
            # Append STATE_IN/STATE_OUT (and time-rank) handler.
            pipeline.append(AddStatesFromEpisodesToBatch())
            # If multi-agent -> Map from AgentID-based data to ModuleID based data.
            if self.is_multi_agent():
                pipeline.append(
                    AgentToModuleMapping(
                        module_specs=(
                            self.rl_module_spec.module_specs
                            if isinstance(self.rl_module_spec, MultiRLModuleSpec)
                            else set(self.policies)
                        ),
                        agent_to_module_mapping_fn=self.policy_mapping_fn,
                    )
                )
            # Batch all data.
            pipeline.append(BatchIndividualItems(multi_agent=self.is_multi_agent()))
            # Convert to Tensors.
            pipeline.append(NumpyToTensor())

        return pipeline

    def build_module_to_env_connector(self, env):
        from ray.rllib.connectors.module_to_env import (
            GetActions,
            ListifyDataForVectorEnv,
            ModuleToAgentUnmapping,
            ModuleToEnvPipeline,
            NormalizeAndClipActions,
            RemoveSingleTsTimeRankFromBatch,
            TensorToNumpy,
            UnBatchToIndividualItems,
        )

        custom_connectors = []
        # Create a module-to-env connector pipeline (including RLlib's default
        # module->env connector piece) and return it.
        if self._module_to_env_connector is not None:
            val_ = self._module_to_env_connector(env)

            from ray.rllib.connectors.connector_v2 import ConnectorV2

            # ConnectorV2 (piece or pipeline).
            if isinstance(val_, ConnectorV2):
                custom_connectors = [val_]
            # Sequence of individual ConnectorV2 pieces.
            elif isinstance(val_, (list, tuple)):
                custom_connectors = list(val_)
            # Unsupported return value.
            else:
                raise ValueError(
                    "`AlgorithmConfig.env_runners(module_to_env_connector=..)` must "
                    "return a ConnectorV2 object or a list thereof (to be added to a "
                    f"pipeline)! Your function returned {val_}."
                )

        pipeline = ModuleToEnvPipeline(
            connectors=custom_connectors,
            input_observation_space=getattr(
                env, "single_observation_space", env.observation_space
            ),
            input_action_space=getattr(env, "single_action_space", env.action_space),
        )

        if self.add_default_connectors_to_module_to_env_pipeline:
            # Prepend: Anything that has to do with plain data processing (not
            # particularly with the actions).

            # Remove extra time-rank, if applicable.
            pipeline.prepend(RemoveSingleTsTimeRankFromBatch())

            # If multi-agent -> Map from ModuleID-based data to AgentID based data.
            if self.is_multi_agent():
                pipeline.prepend(ModuleToAgentUnmapping())

            # Unbatch all data.
            pipeline.prepend(UnBatchToIndividualItems())

            # Convert to numpy.
            pipeline.prepend(TensorToNumpy())

            # Sample actions from ACTION_DIST_INPUTS (if ACTIONS not present).
            pipeline.prepend(GetActions())

            # Append: Anything that has to do with action sampling.
            # Unsquash/clip actions based on config and action space.
            pipeline.append(
                NormalizeAndClipActions(
                    normalize_actions=self.normalize_actions,
                    clip_actions=self.clip_actions,
                )
            )
            # Listify data from ConnectorV2-data format to normal lists that we can
            # index into by env vector index. These lists contain individual items
            # for single-agent and multi-agent dicts for multi-agent.
            pipeline.append(ListifyDataForVectorEnv())

        return pipeline

    def build_learner_connector(
        self,
        input_observation_space,
        input_action_space,
        device=None,
    ):
        from ray.rllib.connectors.learner import (
            AddColumnsFromEpisodesToTrainBatch,
            AddObservationsFromEpisodesToBatch,
            AddStatesFromEpisodesToBatch,
            AgentToModuleMapping,
            BatchIndividualItems,
            LearnerConnectorPipeline,
            NumpyToTensor,
        )

        custom_connectors = []
        # Create a learner connector pipeline (including RLlib's default
        # learner connector piece) and return it.
        if self._learner_connector is not None:
            val_ = self._learner_connector(
                input_observation_space,
                input_action_space,
                # device,  # TODO (sven): Also pass device into custom builder.
            )

            from ray.rllib.connectors.connector_v2 import ConnectorV2

            # ConnectorV2 (piece or pipeline).
            if isinstance(val_, ConnectorV2):
                custom_connectors = [val_]
            # Sequence of individual ConnectorV2 pieces.
            elif isinstance(val_, (list, tuple)):
                custom_connectors = list(val_)
            # Unsupported return value.
            else:
                raise ValueError(
                    "`AlgorithmConfig.training(learner_connector=..)` must return "
                    "a ConnectorV2 object or a list thereof (to be added to a "
                    f"pipeline)! Your function returned {val_}."
                )

        pipeline = LearnerConnectorPipeline(
            connectors=custom_connectors,
            input_observation_space=input_observation_space,
            input_action_space=input_action_space,
        )
        if self.add_default_connectors_to_learner_pipeline:
            # Append OBS handling.
            pipeline.append(
                AddObservationsFromEpisodesToBatch(as_learner_connector=True)
            )
            # Append all other columns handling.
            pipeline.append(AddColumnsFromEpisodesToTrainBatch())
            # Append STATE_IN/STATE_OUT (and time-rank) handler.
            pipeline.append(AddStatesFromEpisodesToBatch(as_learner_connector=True))
            # If multi-agent -> Map from AgentID-based data to ModuleID based data.
            if self.is_multi_agent():
                pipeline.append(
                    AgentToModuleMapping(
                        module_specs=(
                            self.rl_module_spec.module_specs
                            if isinstance(self.rl_module_spec, MultiRLModuleSpec)
                            else set(self.policies)
                        ),
                        agent_to_module_mapping_fn=self.policy_mapping_fn,
                    )
                )
            # Batch all data.
            pipeline.append(BatchIndividualItems(multi_agent=self.is_multi_agent()))
            # Convert to Tensors.
            pipeline.append(NumpyToTensor(as_learner_connector=True, device=device))
        return pipeline

    def build_learner_group(
        self,
        *,
        env: Optional[EnvType] = None,
        spaces: Optional[Dict[ModuleID, Tuple[gym.Space, gym.Space]]] = None,
        rl_module_spec: Optional[RLModuleSpecType] = None,
    ) -> "LearnerGroup":
        """Builds and returns a new LearnerGroup object based on settings in `self`.

        Args:
            env: An optional EnvType object (e.g. a gym.Env) useful for extracting space
                information for the to-be-constructed RLModule inside the LearnerGroup's
                Learner workers. Note that if RLlib cannot infer any space information
                either from this `env` arg, from the optional `spaces` arg or from
                `self`, the LearnerGroup cannot be created.
            spaces: An optional dict mapping ModuleIDs to
                (observation-space, action-space)-tuples for the to-be-constructed
                RLModule inside the LearnerGroup's Learner workers. Note that if RLlib
                cannot infer any space information either from this `spces` arg,
                from the optional `env` arg or from `self`, the LearnerGroup cannot
                be created.
            rl_module_spec: An optional (single-agent or multi-agent) RLModuleSpec to
                use for the constructed LearnerGroup. If None, RLlib will try to infer
                the RLModuleSpec using the other information given and stored in this
                `AlgorithmConfig` object.

        Returns:
            The newly created `LearnerGroup` object.
        """
        from ray.rllib.core.learner.learner_group import LearnerGroup

        # If `spaces` or `env` provided -> Create a MultiRLModuleSpec first to be
        # passed into the LearnerGroup constructor.
        if rl_module_spec is None:
            rl_module_spec = self.get_multi_rl_module_spec(env=env, spaces=spaces)

        # Construct the actual LearnerGroup.
        learner_group = LearnerGroup(config=self.copy(), module_spec=rl_module_spec)

        return learner_group

    def build_learner(
        self,
        *,
        env: Optional[EnvType] = None,
        spaces: Optional[Dict[PolicyID, Tuple[gym.Space, gym.Space]]] = None,
    ) -> "Learner":
        """Builds and returns a new Learner object based on settings in `self`.

        This Learner object will already have its `build()` method called, meaning
        its RLModule will already be constructed.

        Args:
            env: An optional EnvType object (e.g. a gym.Env) useful for extracting space
                information for the to-be-constructed RLModule inside the Learner.
                Note that if RLlib cannot infer any space information
                either from this `env` arg, from the optional `spaces` arg or from
                `self`, the Learner cannot be created.
            spaces: An optional dict mapping ModuleIDs to
                (observation-space, action-space)-tuples for the to-be-constructed
                RLModule inside the Learner. Note that if RLlib cannot infer any
                space information either from this `spces` arg, from the optional
                `env` arg or from `self`, the Learner cannot be created.

        Returns:
            The newly created (and already built) Learner object.
        """
        # If `spaces` or `env` provided -> Create a MultiRLModuleSpec first to be
        # passed into the LearnerGroup constructor.
        rl_module_spec = None
        if env is not None or spaces is not None:
            rl_module_spec = self.get_multi_rl_module_spec(env=env, spaces=spaces)
        # Construct the actual Learner object.
        learner = self.learner_class(config=self, module_spec=rl_module_spec)
        # `build()` the Learner (internal structures such as RLModule, etc..).
        learner.build()

        return learner

    def get_config_for_module(self, module_id: ModuleID) -> "AlgorithmConfig":
        """Returns an AlgorithmConfig object, specific to the given module ID.

        In a multi-agent setup, individual modules might override one or more
        AlgorithmConfig properties (e.g. `train_batch_size`, `lr`) using the
        `overrides()` method.

        In order to retrieve a full AlgorithmConfig instance (with all these overrides
        already translated and built-in), users can call this method with the respective
        module ID.

        Args:
            module_id: The module ID for which to get the final AlgorithmConfig object.

        Returns:
            A new AlgorithmConfig object for the specific module ID.
        """
        # ModuleID NOT found in cached ModuleID, but in overrides dict.
        # Create new algo config object and cache it.
        if (
            module_id not in self._per_module_overrides
            and module_id in self.algorithm_config_overrides_per_module
        ):
            self._per_module_overrides[module_id] = self.copy().update_from_dict(
                self.algorithm_config_overrides_per_module[module_id]
            )

        # Return the module specific algo config object.
        if module_id in self._per_module_overrides:
            return self._per_module_overrides[module_id]
        # No overrides for ModuleID -> return self.
        else:
            return self

    def python_environment(
        self,
        *,
        extra_python_environs_for_driver: Optional[dict] = NotProvided,
        extra_python_environs_for_worker: Optional[dict] = NotProvided,
    ) -> "AlgorithmConfig":
        """Sets the config's python environment settings.

        Args:
            extra_python_environs_for_driver: Any extra python env vars to set in the
                algorithm's process, e.g., {"OMP_NUM_THREADS": "16"}.
            extra_python_environs_for_worker: The extra python environments need to set
                for worker processes.

        Returns:
            This updated AlgorithmConfig object.
        """
        if extra_python_environs_for_driver is not NotProvided:
            self.extra_python_environs_for_driver = extra_python_environs_for_driver
        if extra_python_environs_for_worker is not NotProvided:
            self.extra_python_environs_for_worker = extra_python_environs_for_worker
        return self

    def resources(
        self,
        *,
        num_cpus_for_main_process: Optional[int] = NotProvided,
        num_gpus: Optional[Union[float, int]] = NotProvided,  # @OldAPIStack
        _fake_gpus: Optional[bool] = NotProvided,  # @OldAPIStack
        placement_strategy: Optional[str] = NotProvided,
        # Deprecated args.
        num_cpus_per_worker=DEPRECATED_VALUE,  # moved to `env_runners`
        num_gpus_per_worker=DEPRECATED_VALUE,  # moved to `env_runners`
        custom_resources_per_worker=DEPRECATED_VALUE,  # moved to `env_runners`
        num_learner_workers=DEPRECATED_VALUE,  # moved to `learners`
        num_cpus_per_learner_worker=DEPRECATED_VALUE,  # moved to `learners`
        num_gpus_per_learner_worker=DEPRECATED_VALUE,  # moved to `learners`
        local_gpu_idx=DEPRECATED_VALUE,  # moved to `learners`
        num_cpus_for_local_worker=DEPRECATED_VALUE,
    ) -> "AlgorithmConfig":
        """Specifies resources allocated for an Algorithm and its ray actors/workers.

        Args:
            num_cpus_for_main_process: Number of CPUs to allocate for the main algorithm
                process that runs `Algorithm.training_step()`.
                Note: This is only relevant when running RLlib through Tune. Otherwise,
                `Algorithm.training_step()` runs in the main program (driver).
            num_gpus: Number of GPUs to allocate to the algorithm process.
                Note that not all algorithms can take advantage of GPUs.
                Support for multi-GPU is currently only available for
                tf-[PPO/IMPALA/DQN/PG]. This can be fractional (e.g., 0.3 GPUs).
            _fake_gpus: Set to True for debugging (multi-)?GPU funcitonality on a
                CPU machine. GPU towers will be simulated by graphs located on
                CPUs in this case. Use `num_gpus` to test for different numbers of
                fake GPUs.
            placement_strategy: The strategy for the placement group factory returned by
                `Algorithm.default_resource_request()`. A PlacementGroup defines, which
                devices (resources) should always be co-located on the same node.
                For example, an Algorithm with 2 EnvRunners and 1 Learner (with
                1 GPU) will request a placement group with the bundles:
                [{"cpu": 1}, {"gpu": 1, "cpu": 1}, {"cpu": 1}, {"cpu": 1}], where the
                first bundle is for the local (main Algorithm) process, the second one
                for the 1 Learner worker and the last 2 bundles are for the two
                EnvRunners. These bundles can now be "placed" on the same or different
                nodes depending on the value of `placement_strategy`:
                "PACK": Packs bundles into as few nodes as possible.
                "SPREAD": Places bundles across distinct nodes as even as possible.
                "STRICT_PACK": Packs bundles into one node. The group is not allowed
                to span multiple nodes.
                "STRICT_SPREAD": Packs bundles across distinct nodes.

        Returns:
            This updated AlgorithmConfig object.
        """
        if num_cpus_per_worker != DEPRECATED_VALUE:
            deprecation_warning(
                old="AlgorithmConfig.resources(num_cpus_per_worker)",
                new="AlgorithmConfig.env_runners(num_cpus_per_env_runner)",
                error=False,
            )
            self.num_cpus_per_env_runner = num_cpus_per_worker

        if num_gpus_per_worker != DEPRECATED_VALUE:
            deprecation_warning(
                old="AlgorithmConfig.resources(num_gpus_per_worker)",
                new="AlgorithmConfig.env_runners(num_gpus_per_env_runner)",
                error=False,
            )
            self.num_gpus_per_env_runner = num_gpus_per_worker

        if custom_resources_per_worker != DEPRECATED_VALUE:
            deprecation_warning(
                old="AlgorithmConfig.resources(custom_resources_per_worker)",
                new="AlgorithmConfig.env_runners(custom_resources_per_env_runner)",
                error=False,
            )
            self.custom_resources_per_env_runner = custom_resources_per_worker

        if num_learner_workers != DEPRECATED_VALUE:
            deprecation_warning(
                old="AlgorithmConfig.resources(num_learner_workers)",
                new="AlgorithmConfig.learners(num_learner)",
                error=False,
            )
            self.num_learners = num_learner_workers

        if num_cpus_per_learner_worker != DEPRECATED_VALUE:
            deprecation_warning(
                old="AlgorithmConfig.resources(num_cpus_per_learner_worker)",
                new="AlgorithmConfig.learners(num_cpus_per_learner)",
                error=False,
            )
            self.num_cpus_per_learner = num_cpus_per_learner_worker

        if num_gpus_per_learner_worker != DEPRECATED_VALUE:
            deprecation_warning(
                old="AlgorithmConfig.resources(num_gpus_per_learner_worker)",
                new="AlgorithmConfig.learners(num_gpus_per_learner)",
                error=False,
            )
            self.num_gpus_per_learner = num_gpus_per_learner_worker

        if local_gpu_idx != DEPRECATED_VALUE:
            deprecation_warning(
                old="AlgorithmConfig.resources(local_gpu_idx)",
                new="AlgorithmConfig.learners(local_gpu_idx)",
                error=False,
            )
            self.local_gpu_idx = local_gpu_idx

        if num_cpus_for_local_worker != DEPRECATED_VALUE:
            deprecation_warning(
                old="AlgorithmConfig.resources(num_cpus_for_local_worker)",
                new="AlgorithmConfig.resources(num_cpus_for_main_process)",
                error=False,
            )
            self.num_cpus_for_main_process = num_cpus_for_local_worker

        if num_cpus_for_main_process is not NotProvided:
            self.num_cpus_for_main_process = num_cpus_for_main_process
        if num_gpus is not NotProvided:
            self.num_gpus = num_gpus
        if _fake_gpus is not NotProvided:
            self._fake_gpus = _fake_gpus
        if placement_strategy is not NotProvided:
            self.placement_strategy = placement_strategy

        return self

    def framework(
        self,
        framework: Optional[str] = NotProvided,
        *,
        eager_tracing: Optional[bool] = NotProvided,
        eager_max_retraces: Optional[int] = NotProvided,
        tf_session_args: Optional[Dict[str, Any]] = NotProvided,
        local_tf_session_args: Optional[Dict[str, Any]] = NotProvided,
        torch_compile_learner: Optional[bool] = NotProvided,
        torch_compile_learner_what_to_compile: Optional[str] = NotProvided,
        torch_compile_learner_dynamo_mode: Optional[str] = NotProvided,
        torch_compile_learner_dynamo_backend: Optional[str] = NotProvided,
        torch_compile_worker: Optional[bool] = NotProvided,
        torch_compile_worker_dynamo_backend: Optional[str] = NotProvided,
        torch_compile_worker_dynamo_mode: Optional[str] = NotProvided,
        torch_ddp_kwargs: Optional[Dict[str, Any]] = NotProvided,
        torch_skip_nan_gradients: Optional[bool] = NotProvided,
    ) -> "AlgorithmConfig":
        """Sets the config's DL framework settings.

        Args:
            framework: torch: PyTorch; tf2: TensorFlow 2.x (eager execution or traced
                if eager_tracing=True); tf: TensorFlow (static-graph);
            eager_tracing: Enable tracing in eager mode. This greatly improves
                performance (speedup ~2x), but makes it slightly harder to debug
                since Python code won't be evaluated after the initial eager pass.
                Only possible if framework=tf2.
            eager_max_retraces: Maximum number of tf.function re-traces before a
                runtime error is raised. This is to prevent unnoticed retraces of
                methods inside the `..._eager_traced` Policy, which could slow down
                execution by a factor of 4, without the user noticing what the root
                cause for this slowdown could be.
                Only necessary for framework=tf2.
                Set to None to ignore the re-trace count and never throw an error.
            tf_session_args: Configures TF for single-process operation by default.
            local_tf_session_args: Override the following tf session args on the local
                worker
            torch_compile_learner: If True, forward_train methods on TorchRLModules
                on the learner are compiled. If not specified, the default is to compile
                forward train on the learner.
            torch_compile_learner_what_to_compile: A TorchCompileWhatToCompile
                mode specifying what to compile on the learner side if
                torch_compile_learner is True. See TorchCompileWhatToCompile for
                details and advice on its usage.
            torch_compile_learner_dynamo_backend: The torch dynamo backend to use on
                the learner.
            torch_compile_learner_dynamo_mode: The torch dynamo mode to use on the
                learner.
            torch_compile_worker: If True, forward exploration and inference methods on
                TorchRLModules on the workers are compiled. If not specified,
                the default is to not compile forward methods on the workers because
                retracing can be expensive.
            torch_compile_worker_dynamo_backend: The torch dynamo backend to use on
                the workers.
            torch_compile_worker_dynamo_mode: The torch dynamo mode to use on the
                workers.
            torch_ddp_kwargs: The kwargs to pass into
                `torch.nn.parallel.DistributedDataParallel` when using `num_learners
                > 1`. This is specifically helpful when searching for unused parameters
                that are not used in the backward pass. This can give hints for errors
                in custom models where some parameters do not get touched in the
                backward pass although they should.
            torch_skip_nan_gradients: If updates with `nan` gradients should be entirely
                skipped. This skips updates in the optimizer entirely if they contain
                any `nan` gradient. This can help to avoid biasing moving-average based
                optimizers - like Adam. This can help in training phases where policy
                updates can be highly unstable such as during the early stages of
                training or with highly exploratory policies. In such phases many
                gradients might turn `nan` and setting them to zero could corrupt the
                optimizer's internal state. The default is `False` and turns `nan`
                gradients to zero. If many `nan` gradients are encountered consider (a)
                monitoring gradients by setting `log_gradients` in `AlgorithmConfig` to
                `True`, (b) use proper weight initialization (e.g. Xavier, Kaiming) via
                the `model_config_dict` in `AlgorithmConfig.rl_module` and/or (c)
                gradient clipping via `grad_clip` in `AlgorithmConfig.training`.

        Returns:
            This updated AlgorithmConfig object.
        """
        if framework is not NotProvided:
            if framework == "tfe":
                deprecation_warning(
                    old="AlgorithmConfig.framework('tfe')",
                    new="AlgorithmConfig.framework('tf2')",
                    error=True,
                )
            self.framework_str = framework
        if eager_tracing is not NotProvided:
            self.eager_tracing = eager_tracing
        if eager_max_retraces is not NotProvided:
            self.eager_max_retraces = eager_max_retraces
        if tf_session_args is not NotProvided:
            self.tf_session_args = tf_session_args
        if local_tf_session_args is not NotProvided:
            self.local_tf_session_args = local_tf_session_args

        if torch_compile_learner is not NotProvided:
            self.torch_compile_learner = torch_compile_learner
        if torch_compile_learner_dynamo_backend is not NotProvided:
            self.torch_compile_learner_dynamo_backend = (
                torch_compile_learner_dynamo_backend
            )
        if torch_compile_learner_dynamo_mode is not NotProvided:
            self.torch_compile_learner_dynamo_mode = torch_compile_learner_dynamo_mode
        if torch_compile_learner_what_to_compile is not NotProvided:
            self.torch_compile_learner_what_to_compile = (
                torch_compile_learner_what_to_compile
            )
        if torch_compile_worker is not NotProvided:
            self.torch_compile_worker = torch_compile_worker
        if torch_compile_worker_dynamo_backend is not NotProvided:
            self.torch_compile_worker_dynamo_backend = (
                torch_compile_worker_dynamo_backend
            )
        if torch_compile_worker_dynamo_mode is not NotProvided:
            self.torch_compile_worker_dynamo_mode = torch_compile_worker_dynamo_mode
        if torch_ddp_kwargs is not NotProvided:
            self.torch_ddp_kwargs = torch_ddp_kwargs
        if torch_skip_nan_gradients is not NotProvided:
            self.torch_skip_nan_gradients = torch_skip_nan_gradients

        return self

    def api_stack(
        self,
        enable_rl_module_and_learner: Optional[bool] = NotProvided,
        enable_env_runner_and_connector_v2: Optional[bool] = NotProvided,
    ) -> "AlgorithmConfig":
        """Sets the config's API stack settings.

        Args:
            enable_rl_module_and_learner: Enables the usage of `RLModule` (instead of
                `ModelV2`) and Learner (instead of the training-related parts of
                `Policy`). If `enable_env_runner_and_connector_v2=False`, these two
                classes (`RLModule` and `Learner`) will be used along with
                `RolloutWorkers` and `Policy`.
            enable_env_runner_and_connector_v2: Enables the usage of EnvRunners
                (SingleAgentEnvRunner and MultiAgentEnvRunner) and ConnectorV2.
                When setting this to True, `enable_rl_module_and_learner` must be True
                as well.

        Returns:
            This updated AlgorithmConfig object.
        """
        if enable_rl_module_and_learner is not NotProvided:
            self.enable_rl_module_and_learner = enable_rl_module_and_learner

            if enable_rl_module_and_learner is True and self.exploration_config:
                self.__prior_exploration_config = self.exploration_config
                self.exploration_config = {}

            elif enable_rl_module_and_learner is False and not self.exploration_config:
                if self.__prior_exploration_config is not None:
                    self.exploration_config = self.__prior_exploration_config
                    self.__prior_exploration_config = None
                else:
                    logger.warning(
                        "config.enable_rl_module_and_learner was set to False, but no "
                        "prior exploration config was found to be restored."
                    )

        if enable_env_runner_and_connector_v2 is not NotProvided:
            self.enable_env_runner_and_connector_v2 = enable_env_runner_and_connector_v2

        return self

    def environment(
        self,
        env: Optional[Union[str, EnvType]] = NotProvided,
        *,
        env_config: Optional[EnvConfigDict] = NotProvided,
        observation_space: Optional[gym.spaces.Space] = NotProvided,
        action_space: Optional[gym.spaces.Space] = NotProvided,
        env_task_fn: Optional[
            Callable[[ResultDict, EnvType, EnvContext], Any]
        ] = NotProvided,
        render_env: Optional[bool] = NotProvided,
        clip_rewards: Optional[Union[bool, float]] = NotProvided,
        normalize_actions: Optional[bool] = NotProvided,
        clip_actions: Optional[bool] = NotProvided,
        disable_env_checking: Optional[bool] = NotProvided,
        is_atari: Optional[bool] = NotProvided,
        action_mask_key: Optional[str] = NotProvided,
        # Deprecated args.
        auto_wrap_old_gym_envs=DEPRECATED_VALUE,
    ) -> "AlgorithmConfig":
        """Sets the config's RL-environment settings.

        Args:
            env: The environment specifier. This can either be a tune-registered env,
                via `tune.register_env([name], lambda env_ctx: [env object])`,
                or a string specifier of an RLlib supported type. In the latter case,
                RLlib will try to interpret the specifier as either an Farama-Foundation
                gymnasium env, a PyBullet env, or a fully qualified classpath to an Env
                class, e.g. "ray.rllib.examples.envs.classes.random_env.RandomEnv".
            env_config: Arguments dict passed to the env creator as an EnvContext
                object (which is a dict plus the properties: `num_env_runners`,
                `worker_index`, `vector_index`, and `remote`).
            observation_space: The observation space for the Policies of this Algorithm.
            action_space: The action space for the Policies of this Algorithm.
            env_task_fn: A callable taking the last train results, the base env and the
                env context as args and returning a new task to set the env to.
                The env must be a `TaskSettableEnv` sub-class for this to work.
                See `examples/curriculum_learning.py` for an example.
            render_env: If True, try to render the environment on the local worker or on
                worker 1 (if num_env_runners > 0). For vectorized envs, this usually
                means that only the first sub-environment will be rendered.
                In order for this to work, your env will have to implement the
                `render()` method which either:
                a) handles window generation and rendering itself (returning True) or
                b) returns a numpy uint8 image of shape [height x width x 3 (RGB)].
            clip_rewards: Whether to clip rewards during Policy's postprocessing.
                None (default): Clip for Atari only (r=sign(r)).
                True: r=sign(r): Fixed rewards -1.0, 1.0, or 0.0.
                False: Never clip.
                [float value]: Clip at -value and + value.
                Tuple[value1, value2]: Clip at value1 and value2.
            normalize_actions: If True, RLlib will learn entirely inside a normalized
                action space (0.0 centered with small stddev; only affecting Box
                components). RLlib will unsquash actions (and clip, just in case) to the
                bounds of the env's action space before sending actions back to the env.
            clip_actions: If True, the RLlib default ModuleToEnv connector will clip
                actions according to the env's bounds (before sending them into the
                `env.step()` call).
            disable_env_checking: Disable RLlib's env checks after a gymnasium.Env
                instance has been constructed in an EnvRunner. Note that the checks
                include an `env.reset()` and `env.step()` (with a random action), which
                might tinker with your env's logic and behavior and thus negatively
                influence sample collection- and/or learning behavior.
            is_atari: This config can be used to explicitly specify whether the env is
                an Atari env or not. If not specified, RLlib will try to auto-detect
                this.
            action_mask_key: If observation is a dictionary, expect the value by
                the key `action_mask_key` to contain a valid actions mask (`numpy.int8`
                array of zeros and ones). Defaults to "action_mask".

        Returns:
            This updated AlgorithmConfig object.
        """
        if auto_wrap_old_gym_envs != DEPRECATED_VALUE:
            deprecation_warning(
                old="AlgorithmConfig.environment(auto_wrap_old_gym_envs=..)",
                error=True,
            )
        if env is not NotProvided:
            self.env = env
        if env_config is not NotProvided:
            deep_update(self.env_config, env_config, True)
        if observation_space is not NotProvided:
            self.observation_space = observation_space
        if action_space is not NotProvided:
            self.action_space = action_space
        if env_task_fn is not NotProvided:
            self.env_task_fn = env_task_fn
        if render_env is not NotProvided:
            self.render_env = render_env
        if clip_rewards is not NotProvided:
            self.clip_rewards = clip_rewards
        if normalize_actions is not NotProvided:
            self.normalize_actions = normalize_actions
        if clip_actions is not NotProvided:
            self.clip_actions = clip_actions
        if disable_env_checking is not NotProvided:
            self.disable_env_checking = disable_env_checking
        if is_atari is not NotProvided:
            self._is_atari = is_atari
        if action_mask_key is not NotProvided:
            self.action_mask_key = action_mask_key

        return self

    def env_runners(
        self,
        *,
        env_runner_cls: Optional[type] = NotProvided,
        num_env_runners: Optional[int] = NotProvided,
        num_envs_per_env_runner: Optional[int] = NotProvided,
        num_cpus_per_env_runner: Optional[int] = NotProvided,
        num_gpus_per_env_runner: Optional[Union[float, int]] = NotProvided,
        custom_resources_per_env_runner: Optional[dict] = NotProvided,
        validate_env_runners_after_construction: Optional[bool] = NotProvided,
        sample_timeout_s: Optional[float] = NotProvided,
        env_to_module_connector: Optional[
            Callable[[EnvType], Union["ConnectorV2", List["ConnectorV2"]]]
        ] = NotProvided,
        module_to_env_connector: Optional[
            Callable[[EnvType, "RLModule"], Union["ConnectorV2", List["ConnectorV2"]]]
        ] = NotProvided,
        add_default_connectors_to_env_to_module_pipeline: Optional[bool] = NotProvided,
        add_default_connectors_to_module_to_env_pipeline: Optional[bool] = NotProvided,
        episode_lookback_horizon: Optional[int] = NotProvided,
        use_worker_filter_stats: Optional[bool] = NotProvided,
        update_worker_filter_stats: Optional[bool] = NotProvided,
        compress_observations: Optional[bool] = NotProvided,
        rollout_fragment_length: Optional[Union[int, str]] = NotProvided,
        batch_mode: Optional[str] = NotProvided,
        explore: Optional[bool] = NotProvided,
        # @OldAPIStack settings.
        exploration_config: Optional[dict] = NotProvided,  # @OldAPIStack
        create_env_on_local_worker: Optional[bool] = NotProvided,  # @OldAPIStack
        sample_collector: Optional[Type[SampleCollector]] = NotProvided,  # @OldAPIStack
        enable_connectors: Optional[bool] = NotProvided,  # @OldAPIStack
        remote_worker_envs: Optional[bool] = NotProvided,  # @OldAPIStack
        remote_env_batch_wait_ms: Optional[float] = NotProvided,  # @OldAPIStack
        preprocessor_pref: Optional[str] = NotProvided,  # @OldAPIStack
        observation_filter: Optional[str] = NotProvided,  # @OldAPIStack
        enable_tf1_exec_eagerly: Optional[bool] = NotProvided,  # @OldAPIStack
        sampler_perf_stats_ema_coef: Optional[float] = NotProvided,  # @OldAPIStack
        # Deprecated args.
        num_rollout_workers=DEPRECATED_VALUE,
        num_envs_per_worker=DEPRECATED_VALUE,
        validate_workers_after_construction=DEPRECATED_VALUE,
        ignore_worker_failures=DEPRECATED_VALUE,
        recreate_failed_workers=DEPRECATED_VALUE,
        restart_failed_sub_environments=DEPRECATED_VALUE,
        num_consecutive_worker_failures_tolerance=DEPRECATED_VALUE,
        worker_health_probe_timeout_s=DEPRECATED_VALUE,
        worker_restore_timeout_s=DEPRECATED_VALUE,
        synchronize_filter=DEPRECATED_VALUE,
    ) -> "AlgorithmConfig":
        """Sets the rollout worker configuration.

        Args:
            env_runner_cls: The EnvRunner class to use for environment rollouts (data
                collection).
            num_env_runners: Number of EnvRunner actors to create for parallel sampling.
                Setting this to 0 will force sampling to be done in the local
                EnvRunner (main process or the Algorithm's actor when using Tune).
            num_envs_per_env_runner: Number of environments to step through
                (vector-wise) per EnvRunner. This enables batching when computing
                actions through RLModule inference, which can improve performance
                for inference-bottlenecked workloads.
            num_cpus_per_env_runner: Number of CPUs to allocate per EnvRunner.
            num_gpus_per_env_runner: Number of GPUs to allocate per EnvRunner. This can
                be fractional. This is usually needed only if your env itself requires a
                GPU (i.e., it is a GPU-intensive video game), or model inference is
                unusually expensive.
            custom_resources_per_env_runner: Any custom Ray resources to allocate per
                EnvRunner.
            sample_timeout_s: The timeout in seconds for calling `sample()` on remote
                EnvRunner workers. Results (episode list) from workers that take longer
                than this time are discarded. Only used by algorithms that sample
                synchronously in turn with their update step (e.g. PPO or DQN). Not
                relevant for any algos that sample asynchronously, such as APPO or
                IMPALA.
            sample_collector: For the old API stack only. The SampleCollector class to
                be used to collect and retrieve environment-, model-, and sampler data.
                Override the SampleCollector base class to implement your own
                collection/buffering/retrieval logic.
            create_env_on_local_worker: When `num_env_runners` > 0, the driver
                (local_worker; worker-idx=0) does not need an environment. This is
                because it doesn't have to sample (done by remote_workers;
                worker_indices > 0) nor evaluate (done by evaluation workers;
                see below).
            enable_connectors: Use connector based environment runner, so that all
                preprocessing of obs and postprocessing of actions are done in agent
                and action connectors.
            env_to_module_connector: A callable taking an Env as input arg and returning
                an env-to-module ConnectorV2 (might be a pipeline) object.
            module_to_env_connector: A callable taking an Env and an RLModule as input
                args and returning a module-to-env ConnectorV2 (might be a pipeline)
                object.
            add_default_connectors_to_env_to_module_pipeline: If True (default), RLlib's
                EnvRunners will automatically add the default env-to-module ConnectorV2
                pieces to the EnvToModulePipeline. These automatically perform adding
                observations and states (in case of stateful Module(s)), agent-to-module
                mapping, batching, and conversion to tensor data. Only if you know
                exactly what you are doing, you should set this setting to False.
                Note that this setting is only relevant if the new API stack is used
                (including the new EnvRunner classes).
            add_default_connectors_to_module_to_env_pipeline: If True (default), RLlib's
                EnvRunners will automatically add the default module-to-env ConnectorV2
                pieces to the ModuleToEnvPipeline. These automatically perform removing
                the additional time-rank (if applicable, in case of stateful
                Module(s)), module-to-agent unmapping, un-batching (to lists), and
                conversion from tensor data to numpy. Only if you know exactly what you
                are doing, you should set this setting to False.
                Note that this setting is only relevant if the new API stack is used
                (including the new EnvRunner classes).
            episode_lookback_horizon: The amount of data (in timesteps) to keep from the
                preceeding episode chunk when a new chunk (for the same episode) is
                generated to continue sampling at a later time. The larger this value,
                the more an env-to-module connector will be able to look back in time
                and compile RLModule input data from this information. For example, if
                your custom env-to-module connector (and your custom RLModule) requires
                the previous 10 rewards as inputs, you must set this to at least 10.
            use_worker_filter_stats: Whether to use the workers in the EnvRunnerGroup to
                update the central filters (held by the local worker). If False, stats
                from the workers will not be used and discarded.
            update_worker_filter_stats: Whether to push filter updates from the central
                filters (held by the local worker) to the remote workers' filters.
                Setting this to True might be useful within the evaluation config in
                order to disable the usage of evaluation trajectories for synching
                the central filter (used for training).
            rollout_fragment_length: Divide episodes into fragments of this many steps
                each during sampling. Trajectories of this size are collected from
                EnvRunners and combined into a larger batch of `train_batch_size`
                for learning.
                For example, given rollout_fragment_length=100 and
                train_batch_size=1000:
                1. RLlib collects 10 fragments of 100 steps each from rollout workers.
                2. These fragments are concatenated and we perform an epoch of SGD.
                When using multiple envs per worker, the fragment size is multiplied by
                `num_envs_per_env_runner`. This is since we are collecting steps from
                multiple envs in parallel. For example, if num_envs_per_env_runner=5,
                then EnvRunners will return experiences in chunks of 5*100 = 500 steps.
                The dataflow here can vary per algorithm. For example, PPO further
                divides the train batch into minibatches for multi-epoch SGD.
                Set `rollout_fragment_length` to "auto" to have RLlib compute an exact
                value to match the given batch size.
            batch_mode: How to build individual batches with the EnvRunner(s). Batches
                coming from distributed EnvRunners are usually concat'd to form the
                train batch. Note that "steps" below can mean different things (either
                env- or agent-steps) and depends on the `count_steps_by` setting,
                adjustable via `AlgorithmConfig.multi_agent(count_steps_by=..)`:
                1) "truncate_episodes": Each call to `EnvRunner.sample()` will return a
                batch of at most `rollout_fragment_length * num_envs_per_env_runner` in
                size. The batch will be exactly `rollout_fragment_length * num_envs`
                in size if postprocessing does not change batch sizes. Episodes
                may be truncated in order to meet this size requirement.
                This mode guarantees evenly sized batches, but increases
                variance as the future return must now be estimated at truncation
                boundaries.
                2) "complete_episodes": Each call to `EnvRunner.sample()` will return a
                batch of at least `rollout_fragment_length * num_envs_per_env_runner` in
                size. Episodes will not be truncated, but multiple episodes
                may be packed within one batch to meet the (minimum) batch size.
                Note that when `num_envs_per_env_runner > 1`, episode steps will be
                buffered until the episode completes, and hence batches may contain
                significant amounts of off-policy data.
            explore: Default exploration behavior, iff `explore=None` is passed into
                compute_action(s). Set to False for no exploration behavior (e.g.,
                for evaluation).
            exploration_config: A dict specifying the Exploration object's config.
            remote_worker_envs: If using num_envs_per_env_runner > 1, whether to create
                those new envs in remote processes instead of in the same worker.
                This adds overheads, but can make sense if your envs can take much
                time to step / reset (e.g., for StarCraft). Use this cautiously;
                overheads are significant.
            remote_env_batch_wait_ms: Timeout that remote workers are waiting when
                polling environments. 0 (continue when at least one env is ready) is
                a reasonable default, but optimal value could be obtained by measuring
                your environment step / reset and model inference perf.
            validate_env_runners_after_construction: Whether to validate that each
                created remote EnvRunner is healthy after its construction process.
            preprocessor_pref: Whether to use "rllib" or "deepmind" preprocessors by
                default. Set to None for using no preprocessor. In this case, the
                model will have to handle possibly complex observations from the
                environment.
            observation_filter: Element-wise observation filter, either "NoFilter"
                or "MeanStdFilter".
            compress_observations: Whether to LZ4 compress individual observations
                in the SampleBatches collected during rollouts.
            enable_tf1_exec_eagerly: Explicitly tells the rollout worker to enable
                TF eager execution. This is useful for example when framework is
                "torch", but a TF2 policy needs to be restored for evaluation or
                league-based purposes.
            sampler_perf_stats_ema_coef: If specified, perf stats are in EMAs. This
                is the coeff of how much new data points contribute to the averages.
                Default is None, which uses simple global average instead.
                The EMA update rule is: updated = (1 - ema_coef) * old + ema_coef * new

        Returns:
            This updated AlgorithmConfig object.
        """
        if num_rollout_workers != DEPRECATED_VALUE:
            deprecation_warning(
                old="AlgorithmConfig.env_runners(num_rollout_workers)",
                new="AlgorithmConfig.env_runners(num_env_runners)",
                error=False,
            )
            self.num_env_runners = num_rollout_workers
        if num_envs_per_worker != DEPRECATED_VALUE:
            deprecation_warning(
                old="AlgorithmConfig.env_runners(num_envs_per_worker)",
                new="AlgorithmConfig.env_runners(num_envs_per_env_runner)",
                error=False,
            )
            self.num_envs_per_env_runner = num_envs_per_worker
        if validate_workers_after_construction != DEPRECATED_VALUE:
            deprecation_warning(
                old="AlgorithmConfig.env_runners(validate_workers_after_construction)",
                new="AlgorithmConfig.env_runners(validate_env_runners_after_"
                "construction)",
                error=False,
            )
            self.validate_env_runners_after_construction = (
                validate_workers_after_construction
            )

        if env_runner_cls is not NotProvided:
            self.env_runner_cls = env_runner_cls
        if num_env_runners is not NotProvided:
            self.num_env_runners = num_env_runners
        if num_envs_per_env_runner is not NotProvided:
            if num_envs_per_env_runner <= 0:
                raise ValueError(
                    f"`num_envs_per_env_runner` ({num_envs_per_env_runner}) must be "
                    "larger 0!"
                )
            self.num_envs_per_env_runner = num_envs_per_env_runner

        if num_cpus_per_env_runner is not NotProvided:
            self.num_cpus_per_env_runner = num_cpus_per_env_runner
        if num_gpus_per_env_runner is not NotProvided:
            self.num_gpus_per_env_runner = num_gpus_per_env_runner
        if custom_resources_per_env_runner is not NotProvided:
            self.custom_resources_per_env_runner = custom_resources_per_env_runner

        if sample_timeout_s is not NotProvided:
            self.sample_timeout_s = sample_timeout_s
        if sample_collector is not NotProvided:
            self.sample_collector = sample_collector
        if create_env_on_local_worker is not NotProvided:
            self.create_env_on_local_worker = create_env_on_local_worker
        if enable_connectors is not NotProvided:
            self.enable_connectors = enable_connectors
        if env_to_module_connector is not NotProvided:
            self._env_to_module_connector = env_to_module_connector
        if module_to_env_connector is not NotProvided:
            self._module_to_env_connector = module_to_env_connector
        if add_default_connectors_to_env_to_module_pipeline is not NotProvided:
            self.add_default_connectors_to_env_to_module_pipeline = (
                add_default_connectors_to_env_to_module_pipeline
            )
        if add_default_connectors_to_module_to_env_pipeline is not NotProvided:
            self.add_default_connectors_to_module_to_env_pipeline = (
                add_default_connectors_to_module_to_env_pipeline
            )
        if episode_lookback_horizon is not NotProvided:
            self.episode_lookback_horizon = episode_lookback_horizon
        if use_worker_filter_stats is not NotProvided:
            self.use_worker_filter_stats = use_worker_filter_stats
        if update_worker_filter_stats is not NotProvided:
            self.update_worker_filter_stats = update_worker_filter_stats
        if rollout_fragment_length is not NotProvided:
            if not (
                (
                    isinstance(rollout_fragment_length, int)
                    and rollout_fragment_length > 0
                )
                or rollout_fragment_length == "auto"
            ):
                raise ValueError("`rollout_fragment_length` must be int >0 or 'auto'!")
            self.rollout_fragment_length = rollout_fragment_length
        if batch_mode is not NotProvided:
            if batch_mode not in ["truncate_episodes", "complete_episodes"]:
                raise ValueError(
                    f"`batch_mode` ({batch_mode}) must be one of [truncate_episodes|"
                    "complete_episodes]!"
                )
            self.batch_mode = batch_mode
        if explore is not NotProvided:
            self.explore = explore
        if exploration_config is not NotProvided:
            # Override entire `exploration_config` if `type` key changes.
            # Update, if `type` key remains the same or is not specified.
            new_exploration_config = deep_update(
                {"exploration_config": self.exploration_config},
                {"exploration_config": exploration_config},
                False,
                ["exploration_config"],
                ["exploration_config"],
            )
            self.exploration_config = new_exploration_config["exploration_config"]
        if remote_worker_envs is not NotProvided:
            self.remote_worker_envs = remote_worker_envs
        if remote_env_batch_wait_ms is not NotProvided:
            self.remote_env_batch_wait_ms = remote_env_batch_wait_ms
        if validate_env_runners_after_construction is not NotProvided:
            self.validate_env_runners_after_construction = (
                validate_env_runners_after_construction
            )
        if preprocessor_pref is not NotProvided:
            self.preprocessor_pref = preprocessor_pref
        if observation_filter is not NotProvided:
            self.observation_filter = observation_filter
        if synchronize_filter is not NotProvided:
            self.synchronize_filters = synchronize_filter
        if compress_observations is not NotProvided:
            self.compress_observations = compress_observations
        if enable_tf1_exec_eagerly is not NotProvided:
            self.enable_tf1_exec_eagerly = enable_tf1_exec_eagerly
        if sampler_perf_stats_ema_coef is not NotProvided:
            self.sampler_perf_stats_ema_coef = sampler_perf_stats_ema_coef

        # Deprecated settings.
        if synchronize_filter != DEPRECATED_VALUE:
            deprecation_warning(
                old="AlgorithmConfig.env_runners(synchronize_filter=..)",
                new="AlgorithmConfig.env_runners(update_worker_filter_stats=..)",
                error=True,
            )
        if ignore_worker_failures != DEPRECATED_VALUE:
            deprecation_warning(
                old="ignore_worker_failures is deprecated, and will soon be a no-op",
                error=True,
            )
        if recreate_failed_workers != DEPRECATED_VALUE:
            deprecation_warning(
                old="AlgorithmConfig.env_runners(recreate_failed_workers=..)",
                new="AlgorithmConfig.fault_tolerance(recreate_failed_workers=..)",
                error=True,
            )
        if restart_failed_sub_environments != DEPRECATED_VALUE:
            deprecation_warning(
                old="AlgorithmConfig.env_runners(restart_failed_sub_environments=..)",
                new=(
                    "AlgorithmConfig.fault_tolerance("
                    "restart_failed_sub_environments=..)"
                ),
                error=True,
            )
        if num_consecutive_worker_failures_tolerance != DEPRECATED_VALUE:
            deprecation_warning(
                old=(
                    "AlgorithmConfig.env_runners("
                    "num_consecutive_worker_failures_tolerance=..)"
                ),
                new=(
                    "AlgorithmConfig.fault_tolerance("
                    "num_consecutive_worker_failures_tolerance=..)"
                ),
                error=True,
            )
        if worker_health_probe_timeout_s != DEPRECATED_VALUE:
            deprecation_warning(
                old="AlgorithmConfig.env_runners(worker_health_probe_timeout_s=..)",
                new="AlgorithmConfig.fault_tolerance(worker_health_probe_timeout_s=..)",
                error=True,
            )
        if worker_restore_timeout_s != DEPRECATED_VALUE:
            deprecation_warning(
                old="AlgorithmConfig.env_runners(worker_restore_timeout_s=..)",
                new="AlgorithmConfig.fault_tolerance(worker_restore_timeout_s=..)",
                error=True,
            )

        return self

    def learners(
        self,
        *,
        num_learners: Optional[int] = NotProvided,
        num_cpus_per_learner: Optional[Union[float, int]] = NotProvided,
        num_gpus_per_learner: Optional[Union[float, int]] = NotProvided,
        local_gpu_idx: Optional[int] = NotProvided,
    ):
        """Sets LearnerGroup and Learner worker related configurations.

        Args:
            num_learners: Number of Learner workers used for updating the RLModule.
                A value of 0 means training takes place on a local Learner on main
                process CPUs or 1 GPU (determined by `num_gpus_per_learner`).
                For multi-gpu training, you have to set `num_learners` to > 1 and set
                `num_gpus_per_learner` accordingly (e.g., 4 GPUs total and model fits on
                1 GPU: `num_learners=4; num_gpus_per_learner=1` OR 4 GPUs total and
                model requires 2 GPUs: `num_learners=2; num_gpus_per_learner=2`).
            num_cpus_per_learner: Number of CPUs allocated per Learner worker.
                Only necessary for custom processing pipeline inside each Learner
                requiring multiple CPU cores. Ignored if `num_learners=0`.
            num_gpus_per_learner: Number of GPUs allocated per Learner worker. If
                `num_learners=0`, any value greater than 0 runs the
                training on a single GPU on the main process, while a value of 0 runs
                the training on main process CPUs. If `num_gpus_per_learner` is > 0,
                then you shouldn't change `num_cpus_per_learner` (from its default
                value of 1).
            local_gpu_idx: If `num_gpus_per_learner` > 0, and
                `num_learners` < 2, then RLlib uses this GPU index for training. This is
                an index into the available
                CUDA devices. For example if `os.environ["CUDA_VISIBLE_DEVICES"] = "1"`
                and `local_gpu_idx=0`, RLlib uses the GPU with ID=1 on the node.

        Returns:
            This updated AlgorithmConfig object.
        """
        if num_learners is not NotProvided:
            self.num_learners = num_learners
        if num_cpus_per_learner is not NotProvided:
            self.num_cpus_per_learner = num_cpus_per_learner
        if num_gpus_per_learner is not NotProvided:
            self.num_gpus_per_learner = num_gpus_per_learner
        if local_gpu_idx is not NotProvided:
            self.local_gpu_idx = local_gpu_idx

        return self

    def training(
        self,
        *,
        gamma: Optional[float] = NotProvided,
        lr: Optional[LearningRateOrSchedule] = NotProvided,
        grad_clip: Optional[float] = NotProvided,
        grad_clip_by: Optional[str] = NotProvided,
        train_batch_size: Optional[int] = NotProvided,
        train_batch_size_per_learner: Optional[int] = NotProvided,
        num_epochs: Optional[int] = NotProvided,
        minibatch_size: Optional[int] = NotProvided,
        shuffle_batch_per_epoch: Optional[bool] = NotProvided,
        model: Optional[dict] = NotProvided,
        optimizer: Optional[dict] = NotProvided,
        max_requests_in_flight_per_sampler_worker: Optional[int] = NotProvided,
        learner_class: Optional[Type["Learner"]] = NotProvided,
        learner_connector: Optional[
            Callable[["RLModule"], Union["ConnectorV2", List["ConnectorV2"]]]
        ] = NotProvided,
        add_default_connectors_to_learner_pipeline: Optional[bool] = NotProvided,
        learner_config_dict: Optional[Dict[str, Any]] = NotProvided,
        # Deprecated args.
        num_sgd_iter=DEPRECATED_VALUE,
    ) -> "AlgorithmConfig":
        """Sets the training related configuration.

        Args:
            gamma: Float specifying the discount factor of the Markov Decision process.
            lr: The learning rate (float) or learning rate schedule in the format of
                [[timestep, lr-value], [timestep, lr-value], ...]
                In case of a schedule, intermediary timesteps will be assigned to
                linearly interpolated learning rate values. A schedule config's first
                entry must start with timestep 0, i.e.: [[0, initial_value], [...]].
                Note: If you require a) more than one optimizer (per RLModule),
                b) optimizer types that are not Adam, c) a learning rate schedule that
                is not a linearly interpolated, piecewise schedule as described above,
                or d) specifying c'tor arguments of the optimizer that are not the
                learning rate (e.g. Adam's epsilon), then you must override your
                Learner's `configure_optimizer_for_module()` method and handle
                lr-scheduling yourself.
            grad_clip: If None, no gradient clipping will be applied. Otherwise,
                depending on the setting of `grad_clip_by`, the (float) value of
                `grad_clip` will have the following effect:
                If `grad_clip_by=value`: Will clip all computed gradients individually
                inside the interval [-`grad_clip`, +`grad_clip`].
                If `grad_clip_by=norm`, will compute the L2-norm of each weight/bias
                gradient tensor individually and then clip all gradients such that these
                L2-norms do not exceed `grad_clip`. The L2-norm of a tensor is computed
                via: `sqrt(SUM(w0^2, w1^2, ..., wn^2))` where w[i] are the elements of
                the tensor (no matter what the shape of this tensor is).
                If `grad_clip_by=global_norm`, will compute the square of the L2-norm of
                each weight/bias gradient tensor individually, sum up all these squared
                L2-norms across all given gradient tensors (e.g. the entire module to
                be updated), square root that overall sum, and then clip all gradients
                such that this global L2-norm does not exceed the given value.
                The global L2-norm over a list of tensors (e.g. W and V) is computed
                via:
                `sqrt[SUM(w0^2, w1^2, ..., wn^2) + SUM(v0^2, v1^2, ..., vm^2)]`, where
                w[i] and v[j] are the elements of the tensors W and V (no matter what
                the shapes of these tensors are).
            grad_clip_by: See `grad_clip` for the effect of this setting on gradient
                clipping. Allowed values are `value`, `norm`, and `global_norm`.
            train_batch_size_per_learner: Train batch size per individual Learner
                worker. This setting only applies to the new API stack. The number
                of Learner workers can be set via `config.resources(
                num_learners=...)`. The total effective batch size is then
                `num_learners` x `train_batch_size_per_learner` and you can
                access it with the property `AlgorithmConfig.total_train_batch_size`.
            train_batch_size: Training batch size, if applicable. When on the new API
                stack, this setting should no longer be used. Instead, use
                `train_batch_size_per_learner` (in combination with
                `num_learners`).
            num_epochs: The number of complete passes over the entire train batch (per
                Learner). Each pass might be further split into n minibatches (if
                `minibatch_size` provided).
            minibatch_size: The size of minibatches to use to further split the train
                batch into.
            shuffle_batch_per_epoch: Whether to shuffle the train batch once per epoch.
                If the train batch has a time rank (axis=1), shuffling will only take
                place along the batch axis to not disturb any intact (episode)
                trajectories.
            model: Arguments passed into the policy model. See models/catalog.py for a
                full list of the available model options.
                TODO: Provide ModelConfig objects instead of dicts.
            optimizer: Arguments to pass to the policy optimizer. This setting is not
                used when `enable_rl_module_and_learner=True`.
            max_requests_in_flight_per_sampler_worker: Max number of inflight requests
                to each sampling worker. See the FaultTolerantActorManager class for
                more details.
                Tuning these values is important when running experimens with
                large sample batches, where there is the risk that the object store may
                fill up, causing spilling of objects to disk. This can cause any
                asynchronous requests to become very slow, making your experiment run
                slow as well. You can inspect the object store during your experiment
                via a call to ray memory on your headnode, and by using the ray
                dashboard. If you're seeing that the object store is filling up,
                turn down the number of remote requests in flight, or enable compression
                in your experiment of timesteps.
            learner_class: The `Learner` class to use for (distributed) updating of the
                RLModule. Only used when `enable_rl_module_and_learner=True`.
            learner_connector: A callable taking an env observation space and an env
                action space as inputs and returning a learner ConnectorV2 (might be
                a pipeline) object.
            add_default_connectors_to_learner_pipeline: If True (default), RLlib's
                Learners will automatically add the default Learner ConnectorV2
                pieces to the LearnerPipeline. These automatically perform:
                a) adding observations from episodes to the train batch, if this has not
                already been done by a user-provided connector piece
                b) if RLModule is stateful, add a time rank to the train batch, zero-pad
                the data, and add the correct state inputs, if this has not already been
                done by a user-provided connector piece.
                c) add all other information (actions, rewards, terminateds, etc..) to
                the train batch, if this has not already been done by a user-provided
                connector piece.
                Only if you know exactly what you are doing, you
                should set this setting to False.
                Note that this setting is only relevant if the new API stack is used
                (including the new EnvRunner classes).
            learner_config_dict: A dict to insert any settings accessible from within
                the Learner instance. This should only be used in connection with custom
                Learner subclasses and in case the user doesn't want to write an extra
                `AlgorithmConfig` subclass just to add a few settings to the base Algo's
                own config class.

        Returns:
            This updated AlgorithmConfig object.
        """
        if num_sgd_iter != DEPRECATED_VALUE:
            deprecation_warning(
                old="config.training(num_sgd_iter=..)",
                new="config.training(num_epochs=..)",
                error=False,
            )
            num_epochs = num_sgd_iter

        if gamma is not NotProvided:
            self.gamma = gamma
        if lr is not NotProvided:
            self.lr = lr
        if grad_clip is not NotProvided:
            self.grad_clip = grad_clip
        if grad_clip_by is not NotProvided:
            if grad_clip_by not in ["value", "norm", "global_norm"]:
                raise ValueError(
                    f"`grad_clip_by` ({grad_clip_by}) must be one of: 'value', 'norm', "
                    "or 'global_norm'!"
                )
            self.grad_clip_by = grad_clip_by
        if train_batch_size_per_learner is not NotProvided:
            self.train_batch_size_per_learner = train_batch_size_per_learner
        if train_batch_size is not NotProvided:
            self.train_batch_size = train_batch_size
        if num_epochs is not NotProvided:
            self.num_epochs = num_epochs
        if minibatch_size is not NotProvided:
            self.minibatch_size = minibatch_size
        if shuffle_batch_per_epoch is not NotProvided:
            self.shuffle_batch_per_epoch = shuffle_batch_per_epoch

        if model is not NotProvided:
            self.model.update(model)
            if (
                model.get("_use_default_native_models", DEPRECATED_VALUE)
                != DEPRECATED_VALUE
            ):
                deprecation_warning(
                    old="AlgorithmConfig.training(_use_default_native_models=True)",
                    help="_use_default_native_models is not supported "
                    "anymore. To get rid of this error, set `config.api_stack("
                    "enable_rl_module_and_learner=True)`. Native models will "
                    "be better supported by the upcoming RLModule API.",
                    # Error out if user tries to enable this.
                    error=model["_use_default_native_models"],
                )

        if optimizer is not NotProvided:
            self.optimizer = merge_dicts(self.optimizer, optimizer)
        if max_requests_in_flight_per_sampler_worker is not NotProvided:
            self.max_requests_in_flight_per_sampler_worker = (
                max_requests_in_flight_per_sampler_worker
            )
        if learner_class is not NotProvided:
            self._learner_class = learner_class
        if learner_connector is not NotProvided:
            self._learner_connector = learner_connector
        if add_default_connectors_to_learner_pipeline is not NotProvided:
            self.add_default_connectors_to_learner_pipeline = (
                add_default_connectors_to_learner_pipeline
            )
        if learner_config_dict is not NotProvided:
            self.learner_config_dict.update(learner_config_dict)

        return self

    def callbacks(self, callbacks_class) -> "AlgorithmConfig":
        """Sets the callbacks configuration.

        Args:
            callbacks_class: Callbacks class, whose methods will be run during
                various phases of training and environment sample collection.
                See the `DefaultCallbacks` class and
                `examples/metrics/custom_metrics_and_callbacks.py` for more usage
                information.

        Returns:
            This updated AlgorithmConfig object.
        """
        if callbacks_class is None:
            callbacks_class = DefaultCallbacks
        # Check, whether given `callbacks` is a callable.
        if not callable(callbacks_class):
            raise ValueError(
                "`config.callbacks_class` must be a callable method that "
                "returns a subclass of DefaultCallbacks, got "
                f"{callbacks_class}!"
            )
        self.callbacks_class = callbacks_class

        return self

    def evaluation(
        self,
        *,
        evaluation_interval: Optional[int] = NotProvided,
        evaluation_duration: Optional[Union[int, str]] = NotProvided,
        evaluation_duration_unit: Optional[str] = NotProvided,
        evaluation_sample_timeout_s: Optional[float] = NotProvided,
        evaluation_parallel_to_training: Optional[bool] = NotProvided,
        evaluation_force_reset_envs_before_iteration: Optional[bool] = NotProvided,
        evaluation_config: Optional[
            Union["AlgorithmConfig", PartialAlgorithmConfigDict]
        ] = NotProvided,
        off_policy_estimation_methods: Optional[Dict] = NotProvided,
        ope_split_batch_by_episode: Optional[bool] = NotProvided,
        evaluation_num_env_runners: Optional[int] = NotProvided,
        custom_evaluation_function: Optional[Callable] = NotProvided,
        # Deprecated args.
        always_attach_evaluation_results=DEPRECATED_VALUE,
        evaluation_num_workers=DEPRECATED_VALUE,
    ) -> "AlgorithmConfig":
        """Sets the config's evaluation settings.

        Args:
            evaluation_interval: Evaluate with every `evaluation_interval` training
                iterations. The evaluation stats will be reported under the "evaluation"
                metric key. Set to None (or 0) for no evaluation.
            evaluation_duration: Duration for which to run evaluation each
                `evaluation_interval`. The unit for the duration can be set via
                `evaluation_duration_unit` to either "episodes" (default) or
                "timesteps". If using multiple evaluation workers (EnvRunners) in the
                `evaluation_num_env_runners > 1` setting, the amount of
                episodes/timesteps to run will be split amongst these.
                A special value of "auto" can be used in case
                `evaluation_parallel_to_training=True`. This is the recommended way when
                trying to save as much time on evaluation as possible. The Algorithm
                will then run as many timesteps via the evaluation workers as possible,
                while not taking longer than the parallely running training step and
                thus, never wasting any idle time on either training- or evaluation
                workers. When using this setting (`evaluation_duration="auto"`), it is
                strongly advised to set `evaluation_interval=1` and
                `evaluation_force_reset_envs_before_iteration=True` at the same time.
            evaluation_duration_unit: The unit, with which to count the evaluation
                duration. Either "episodes" (default) or "timesteps". Note that this
                setting is ignored if `evaluation_duration="auto"`.
            evaluation_sample_timeout_s: The timeout (in seconds) for evaluation workers
                to sample a complete episode in the case your config settings are:
                `evaluation_duration != auto` and `evaluation_duration_unit=episode`.
                After this time, the user will receive a warning and instructions on how
                to fix the issue.
            evaluation_parallel_to_training: Whether to run evaluation in parallel to
                the `Algorithm.training_step()` call, using threading. Default=False.
                E.g. for evaluation_interval=1 -> In every call to `Algorithm.train()`,
                the `Algorithm.training_step()` and `Algorithm.evaluate()` calls will
                run in parallel. Note that this setting - albeit extremely efficient b/c
                it wastes no extra time for evaluation - causes the evaluation results
                to lag one iteration behind the rest of the training results. This is
                important when picking a good checkpoint. For example, if iteration 42
                reports a good evaluation `episode_return_mean`, be aware that these
                results were achieved on the weights trained in iteration 41, so you
                should probably pick the iteration 41 checkpoint instead.
            evaluation_force_reset_envs_before_iteration: Whether all environments
                should be force-reset (even if they are not done yet) right before
                the evaluation step of the iteration begins. Setting this to True
                (default) will make sure that the evaluation results will not be
                polluted with episode statistics that were actually (at least partially)
                achieved with an earlier set of weights. Note that this setting is only
                supported on the new API stack w/ EnvRunners and ConnectorV2
                (`config.enable_rl_module_and_learner=True` AND
                `config.enable_env_runner_and_connector_v2=True`).
            evaluation_config: Typical usage is to pass extra args to evaluation env
                creator and to disable exploration by computing deterministic actions.
                IMPORTANT NOTE: Policy gradient algorithms are able to find the optimal
                policy, even if this is a stochastic one. Setting "explore=False" here
                will result in the evaluation workers not using this optimal policy!
            off_policy_estimation_methods: Specify how to evaluate the current policy,
                along with any optional config parameters. This only has an effect when
                reading offline experiences ("input" is not "sampler").
                Available keys:
                {ope_method_name: {"type": ope_type, ...}} where `ope_method_name`
                is a user-defined string to save the OPE results under, and
                `ope_type` can be any subclass of OffPolicyEstimator, e.g.
                ray.rllib.offline.estimators.is::ImportanceSampling
                or your own custom subclass, or the full class path to the subclass.
                You can also add additional config arguments to be passed to the
                OffPolicyEstimator in the dict, e.g.
                {"qreg_dr": {"type": DoublyRobust, "q_model_type": "qreg", "k": 5}}
            ope_split_batch_by_episode: Whether to use SampleBatch.split_by_episode() to
                split the input batch to episodes before estimating the ope metrics. In
                case of bandits you should make this False to see improvements in ope
                evaluation speed. In case of bandits, it is ok to not split by episode,
                since each record is one timestep already. The default is True.
            evaluation_num_env_runners: Number of parallel EnvRunners to use for
                evaluation. Note that this is set to zero by default, which means
                evaluation will be run in the algorithm process (only if
                `evaluation_interval` is not 0 or None). If you increase this, it will
                increase the Ray resource usage of the algorithm since evaluation
                workers are created separately from those EnvRunners used to sample data
                for training.
            custom_evaluation_function: Customize the evaluation method. This must be a
                function of signature (algo: Algorithm, eval_workers: EnvRunnerGroup) ->
                (metrics: dict, env_steps: int, agent_steps: int) (metrics: dict if
                `enable_env_runner_and_connector_v2=True`), where `env_steps` and
                `agent_steps` define the number of sampled steps during the evaluation
                iteration. See the Algorithm.evaluate() method to see the default
                implementation. The Algorithm guarantees all eval workers have the
                latest policy state before this function is called.

        Returns:
            This updated AlgorithmConfig object.
        """
        if always_attach_evaluation_results != DEPRECATED_VALUE:
            deprecation_warning(
                old="AlgorithmConfig.evaluation(always_attach_evaluation_results=..)",
                help="This setting is no longer needed, b/c Tune does not error "
                "anymore (only warns) when a metrics key can't be found in the "
                "results.",
                error=True,
            )
        if evaluation_num_workers != DEPRECATED_VALUE:
            deprecation_warning(
                old="AlgorithmConfig.evaluation(evaluation_num_workers=..)",
                new="AlgorithmConfig.evaluation(evaluation_num_env_runners=..)",
                error=False,
            )
            self.evaluation_num_env_runners = evaluation_num_workers

        if evaluation_interval is not NotProvided:
            self.evaluation_interval = evaluation_interval
        if evaluation_duration is not NotProvided:
            self.evaluation_duration = evaluation_duration
        if evaluation_duration_unit is not NotProvided:
            self.evaluation_duration_unit = evaluation_duration_unit
        if evaluation_sample_timeout_s is not NotProvided:
            self.evaluation_sample_timeout_s = evaluation_sample_timeout_s
        if evaluation_parallel_to_training is not NotProvided:
            self.evaluation_parallel_to_training = evaluation_parallel_to_training
        if evaluation_force_reset_envs_before_iteration is not NotProvided:
            self.evaluation_force_reset_envs_before_iteration = (
                evaluation_force_reset_envs_before_iteration
            )
        if evaluation_config is not NotProvided:
            # If user really wants to set this to None, we should allow this here,
            # instead of creating an empty dict.
            if evaluation_config is None:
                self.evaluation_config = None
            # Update (don't replace) the existing overrides with the provided ones.
            else:
                from ray.rllib.algorithms.algorithm import Algorithm

                self.evaluation_config = deep_update(
                    self.evaluation_config or {},
                    evaluation_config,
                    True,
                    Algorithm._allow_unknown_subkeys,
                    Algorithm._override_all_subkeys_if_type_changes,
                    Algorithm._override_all_key_list,
                )
        if off_policy_estimation_methods is not NotProvided:
            self.off_policy_estimation_methods = off_policy_estimation_methods
        if evaluation_num_env_runners is not NotProvided:
            self.evaluation_num_env_runners = evaluation_num_env_runners
        if custom_evaluation_function is not NotProvided:
            self.custom_evaluation_function = custom_evaluation_function
        if ope_split_batch_by_episode is not NotProvided:
            self.ope_split_batch_by_episode = ope_split_batch_by_episode

        return self

    def offline_data(
        self,
        *,
        input_: Optional[Union[str, Callable[[IOContext], InputReader]]] = NotProvided,
        input_read_method: Optional[Union[str, Callable]] = NotProvided,
        input_read_method_kwargs: Optional[Dict] = NotProvided,
        input_read_schema: Optional[Dict[str, str]] = NotProvided,
        input_read_episodes: Optional[bool] = NotProvided,
        input_read_sample_batches: Optional[bool] = NotProvided,
        input_filesystem: Optional[str] = NotProvided,
        input_filesystem_kwargs: Optional[Dict] = NotProvided,
        input_compress_columns: Optional[List[str]] = NotProvided,
        materialize_data: Optional[bool] = NotProvided,
        materialize_mapped_data: Optional[bool] = NotProvided,
        map_batches_kwargs: Optional[Dict] = NotProvided,
        iter_batches_kwargs: Optional[Dict] = NotProvided,
        prelearner_class: Optional[Type] = NotProvided,
        prelearner_buffer_class: Optional[Type] = NotProvided,
        prelearner_buffer_kwargs: Optional[Dict] = NotProvided,
        prelearner_module_synch_period: Optional[int] = NotProvided,
        dataset_num_iters_per_learner: Optional[int] = NotProvided,
        input_config: Optional[Dict] = NotProvided,
        actions_in_input_normalized: Optional[bool] = NotProvided,
        postprocess_inputs: Optional[bool] = NotProvided,
        shuffle_buffer_size: Optional[int] = NotProvided,
        output: Optional[str] = NotProvided,
        output_config: Optional[Dict] = NotProvided,
        output_compress_columns: Optional[List[str]] = NotProvided,
        output_max_file_size: Optional[float] = NotProvided,
        output_max_rows_per_file: Optional[int] = NotProvided,
        output_write_method: Optional[str] = NotProvided,
        output_write_method_kwargs: Optional[Dict] = NotProvided,
        output_filesystem: Optional[str] = NotProvided,
        output_filesystem_kwargs: Optional[Dict] = NotProvided,
        output_write_episodes: Optional[bool] = NotProvided,
        offline_sampling: Optional[str] = NotProvided,
    ) -> "AlgorithmConfig":
        """Sets the config's offline data settings.

        Args:
            input_: Specify how to generate experiences:
                - "sampler": Generate experiences via online (env) simulation (default).
                - A local directory or file glob expression (e.g., "/tmp/*.json").
                - A list of individual file paths/URIs (e.g., ["/tmp/1.json",
                "s3://bucket/2.json"]).
                - A dict with string keys and sampling probabilities as values (e.g.,
                {"sampler": 0.4, "/tmp/*.json": 0.4, "s3://bucket/expert.json": 0.2}).
                - A callable that takes an `IOContext` object as only arg and returns a
                `ray.rllib.offline.InputReader`.
                - A string key that indexes a callable with
                `tune.registry.register_input`
            input_read_method: Read method for the `ray.data.Dataset` to read in the
                offline data from `input_`. The default is `read_parquet` for Parquet
                files. See https://docs.ray.io/en/latest/data/api/input_output.html for
                more info about available read methods in `ray.data`.
            input_read_method_kwargs: Keyword args for `input_read_method`. These
                will be passed into the read method without checking. If no arguments
                are passed in the default argument
                `{'override_num_blocks': max(num_learners * 2, 2)}` is used. Use these
                keyword args together with `map_batches_kwargs` and
                `iter_batches_kwargs` to tune the performance of the data pipeline.
            input_read_schema: Table schema for converting offline data to episodes.
                This schema maps the offline data columns to
                ray.rllib.core.columns.Columns:
                `{Columns.OBS: 'o_t', Columns.ACTIONS: 'a_t', ...}`. Columns in
                the data set that are not mapped via this schema are sorted into
                episodes' `extra_model_outputs`. If no schema is passed in the default
                schema used is `ray.rllib.offline.offline_data.SCHEMA`. If your data set
                contains already the names in this schema, no `input_read_schema` is
                needed.
            input_read_episodes: Whether offline data is already stored in RLlib's
                `EpisodeType` format, i.e. `ray.rllib.env.SingleAgentEpisode` (multi
                -agent is planned but not supported, yet). Reading episodes directly
                avoids additional transform steps and is usually faster and
                therefore the recommended format when your application remains fully
                inside of RLlib's schema. The other format is a columnar format and is
                agnostic to the RL framework used. Use the latter format, if you are
                unsure when to use the data or in which RL framework. The default is
                to read column data, i.e. False. `input_read_episodes` and
                `input_read_sample_batches` cannot be True at the same time. See
                also `output_write_episodes` to define the output data format when
                recording.
            input_read_sample_batches: Whether offline data is stored in RLlib's old
                stack `SampleBatch` type. This is usually the case for older data
                recorded with RLlib in JSON line format. Reading in `SampleBatch`
                data needs extra transforms and might not concatenate episode chunks
                contained in different `SampleBatch`es in the data. If possible avoid
                to read `SampleBatch`es and convert them in a controlled form into
                RLlib's `EpisodeType` (i.e. `SingleAgentEpisode` or
                `MultiAgentEpisode`). The default is False. `input_read_episodes`
                and `input_read_sample_batches` cannot be True at the same time.
            input_filesystem: A cloud filesystem to handle access to cloud storage when
                reading experiences. Should be either "gcs" for Google Cloud Storage,
                "s3" for AWS S3 buckets, or "abs" for Azure Blob Storage.
            input_filesystem_kwargs: A dictionary holding the kwargs for the filesystem
                given by `input_filesystem`. See `gcsfs.GCSFilesystem` for GCS,
                `pyarrow.fs.S3FileSystem`, for S3, and `ablfs.AzureBlobFilesystem` for
                ABS filesystem arguments.
            input_compress_columns: What input columns are compressed with LZ4 in the
                input data. If data is stored in RLlib's `SingleAgentEpisode` (
                `MultiAgentEpisode` not supported, yet). Note,
                `rllib.core.columns.Columns.OBS` will also try to decompress
                `rllib.core.columns.Columns.NEXT_OBS`.
            materialize_data: Whether the raw data should be materialized in memory.
                This boosts performance, but requires enough memory to avoid an OOM, so
                make sure that your cluster has the resources available. For very large
                data you might want to switch to streaming mode by setting this to
                `False` (default). If your algorithm does not need the RLModule in the
                Learner connector pipeline or all (learner) connectors are stateless
                you should consider setting `materialize_mapped_data` to `True`
                instead (and set `materialize_data` to `False`). If your data does not
                fit into memory and your Learner connector pipeline requires an RLModule
                or is stateful, set both `materialize_data` and
                `materialize_mapped_data` to `False`.
            materialize_mapped_data: Whether the data should be materialized after
                running it through the Learner connector pipeline (i.e. after running
                the `OfflinePreLearner`). This improves performance, but should only be
                used in case the (learner) connector pipeline does not require an
                RLModule and the (learner) connector pipeline is stateless. For example,
                MARWIL's Learner connector pipeline requires the RLModule for value
                function predictions and training batches would become stale after some
                iterations causing learning degradation or divergence. Also ensure that
                your cluster has enough memory available to avoid an OOM. If set to
                `True` (True), make sure that `materialize_data` is set to `False` to
                avoid materialization of two datasets. If your data does not fit into
                memory and your Learner connector pipeline requires an RLModule or is
                stateful, set both `materialize_data` and `materialize_mapped_data` to
                `False`.
            map_batches_kwargs: Keyword args for the `map_batches` method. These will be
                passed into the `ray.data.Dataset.map_batches` method when sampling
                without checking. If no arguments passed in the default arguments
                `{'concurrency': max(2, num_learners), 'zero_copy_batch': True}` is
                used. Use these keyword args together with `input_read_method_kwargs`
                and `iter_batches_kwargs` to tune the performance of the data pipeline.
            iter_batches_kwargs: Keyword args for the `iter_batches` method. These will
                be passed into the `ray.data.Dataset.iter_batches` method when sampling
                without checking. If no arguments are passed in, the default argument
                `{'prefetch_batches': 2, 'local_buffer_shuffle_size':
                train_batch_size_per_learner x 4}` is used. Use these keyword args
                together with `input_read_method_kwargs` and `map_batches_kwargs` to
                tune the performance of the data pipeline.
            prelearner_class: An optional `OfflinePreLearner` class that is used to
                transform data batches in `ray.data.map_batches` used in the
                `OfflineData` class to transform data from columns to batches that can
                be used in the `Learner.update...()` methods. Override the
                `OfflinePreLearner` class and pass your derived class in here, if you
                need to make some further transformations specific for your data or
                loss. The default is None which uses the base `OfflinePreLearner`
                defined in `ray.rllib.offline.offline_prelearner`.
            prelearner_module_synch_period: The period (number of batches converted)
                after which the `RLModule` held by the `PreLearner` should sync weights.
                The `PreLearner` is used to preprocess batches for the learners. The
                higher this value the more off-policy the `PreLearner`'s module will be.
                Values too small will force the `PreLearner` to sync more frequently
                and thus might slow down the data pipeline. The default value chosen
                by the `OfflinePreLearner` is 10.
            dataset_num_iters_per_learner: Number of updates to run in each learner
                during a single training iteration. If None, each learner runs a
                complete epoch over its data block (the dataset is partitioned into
                at least as many blocks as there are learners). The default is `None`.
            input_config: Arguments that describe the settings for reading the input.
                If input is "sample", this will be environment configuration, e.g.
                `env_name` and `env_config`, etc. See `EnvContext` for more info.
                If the input is "dataset", this will be e.g. `format`, `path`.
            actions_in_input_normalized: True, if the actions in a given offline "input"
                are already normalized (between -1.0 and 1.0). This is usually the case
                when the offline file has been generated by another RLlib algorithm
                (e.g. PPO or SAC), while "normalize_actions" was set to True.
            postprocess_inputs: Whether to run postprocess_trajectory() on the
                trajectory fragments from offline inputs. Note that postprocessing will
                be done using the *current* policy, not the *behavior* policy, which
                is typically undesirable for on-policy algorithms.
            shuffle_buffer_size: If positive, input batches will be shuffled via a
                sliding window buffer of this number of batches. Use this if the input
                data is not in random enough order. Input is delayed until the shuffle
                buffer is filled.
            output: Specify where experiences should be saved:
                 - None: don't save any experiences
                 - "logdir" to save to the agent log dir
                 - a path/URI to save to a custom output directory (e.g., "s3://bckt/")
                 - a function that returns a rllib.offline.OutputWriter
            output_config: Arguments accessible from the IOContext for configuring
                custom output.
            output_compress_columns: What sample batch columns to LZ4 compress in the
                output data. Note, `rllib.core.columns.Columns.OBS` will also compress
                `rllib.core.columns.Columns.NEXT_OBS`.
            output_max_file_size: Max output file size (in bytes) before rolling over
                to a new file.
            output_max_rows_per_file: Max output row numbers before rolling over to a
                new file.
            output_write_method: Write method for the `ray.data.Dataset` to write the
                offline data to `output`. The default is `read_parquet` for Parquet
                files. See https://docs.ray.io/en/latest/data/api/input_output.html for
                more info about available read methods in `ray.data`.
            output_write_method_kwargs: `kwargs` for the `output_write_method`. These
                will be passed into the write method without checking.
            output_filesystem: A cloud filesystem to handle access to cloud storage when
                writing experiences. Should be either "gcs" for Google Cloud Storage,
                "s3" for AWS S3 buckets, or "abs" for Azure Blob Storage.
            output_filesystem_kwargs: A dictionary holding the kwargs for the filesystem
                given by `output_filesystem`. See `gcsfs.GCSFilesystem` for GCS,
                `pyarrow.fs.S3FileSystem`, for S3, and `ablfs.AzureBlobFilesystem` for
                ABS filesystem arguments.
            offline_sampling: Whether sampling for the Algorithm happens via
                reading from offline data. If True, EnvRunners will NOT limit the
                number of collected batches within the same `sample()` call based on
                the number of sub-environments within the worker (no sub-environments
                present).

        Returns:
            This updated AlgorithmConfig object.
        """
        if input_ is not NotProvided:
            self.input_ = input_
        if input_read_method is not NotProvided:
            self.input_read_method = input_read_method
        if input_read_method_kwargs is not NotProvided:
            self.input_read_method_kwargs = input_read_method_kwargs
        if input_read_schema is not NotProvided:
            self.input_read_schema = input_read_schema
        if input_read_episodes is not NotProvided:
            self.input_read_episodes = input_read_episodes
        if input_read_sample_batches is not NotProvided:
            self.input_read_sample_batches = input_read_sample_batches
        if input_filesystem is not NotProvided:
            self.input_filesystem = input_filesystem
        if input_filesystem_kwargs is not NotProvided:
            self.input_filesystem_kwargs = input_filesystem_kwargs
        if input_compress_columns is not NotProvided:
            self.input_compress_columns = input_compress_columns
        if materialize_data is not NotProvided:
            self.materialize_data = materialize_data
        if materialize_mapped_data is not NotProvided:
            self.materialize_mapped_data = materialize_mapped_data
        if map_batches_kwargs is not NotProvided:
            self.map_batches_kwargs = map_batches_kwargs
        if iter_batches_kwargs is not NotProvided:
            self.iter_batches_kwargs = iter_batches_kwargs
        if prelearner_class is not NotProvided:
            self.prelearner_class = prelearner_class
        if prelearner_buffer_class is not NotProvided:
            self.prelearner_buffer_class = prelearner_buffer_class
        if prelearner_buffer_kwargs is not NotProvided:
            self.prelearner_buffer_kwargs = prelearner_buffer_kwargs
        if prelearner_module_synch_period is not NotProvided:
            self.prelearner_module_synch_period = prelearner_module_synch_period
        if dataset_num_iters_per_learner is not NotProvided:
            self.dataset_num_iters_per_learner = dataset_num_iters_per_learner
        if input_config is not NotProvided:
            if not isinstance(input_config, dict):
                raise ValueError(
                    f"input_config must be a dict, got {type(input_config)}."
                )
            # TODO (Kourosh) Once we use a complete separation between rollout worker
            #  and input dataset reader we can remove this.
            #  For now Error out if user attempts to set these parameters.
            msg = "{} should not be set in the input_config. RLlib will use {} instead."
            if input_config.get("num_cpus_per_read_task") is not None:
                raise ValueError(
                    msg.format(
                        "num_cpus_per_read_task",
                        "config.env_runners(num_cpus_per_env_runner=..)",
                    )
                )
            if input_config.get("parallelism") is not None:
                if self.in_evaluation:
                    raise ValueError(
                        msg.format(
                            "parallelism",
                            "config.evaluation(evaluation_num_env_runners=..)",
                        )
                    )
                else:
                    raise ValueError(
                        msg.format(
                            "parallelism", "config.env_runners(num_env_runners=..)"
                        )
                    )
            self.input_config = input_config
        if actions_in_input_normalized is not NotProvided:
            self.actions_in_input_normalized = actions_in_input_normalized
        if postprocess_inputs is not NotProvided:
            self.postprocess_inputs = postprocess_inputs
        if shuffle_buffer_size is not NotProvided:
            self.shuffle_buffer_size = shuffle_buffer_size
        if output is not NotProvided:
            self.output = output
        if output_config is not NotProvided:
            self.output_config = output_config
        if output_compress_columns is not NotProvided:
            self.output_compress_columns = output_compress_columns
        if output_max_file_size is not NotProvided:
            self.output_max_file_size = output_max_file_size
        if output_max_rows_per_file is not NotProvided:
            self.output_max_rows_per_file = output_max_rows_per_file
        if output_write_method is not NotProvided:
            self.output_write_method = output_write_method
        if output_write_method_kwargs is not NotProvided:
            self.output_write_method_kwargs = output_write_method_kwargs
        if output_filesystem is not NotProvided:
            self.output_filesystem = output_filesystem
        if output_filesystem_kwargs is not NotProvided:
            self.output_filesystem_kwargs = output_filesystem_kwargs
        if output_write_episodes is not NotProvided:
            self.output_write_episodes = output_write_episodes
        if offline_sampling is not NotProvided:
            self.offline_sampling = offline_sampling

        return self

    def multi_agent(
        self,
        *,
        policies: Optional[
            Union[MultiAgentPolicyConfigDict, Collection[PolicyID]]
        ] = NotProvided,
        policy_map_capacity: Optional[int] = NotProvided,
        policy_mapping_fn: Optional[
            Callable[[AgentID, "OldEpisode"], PolicyID]
        ] = NotProvided,
        policies_to_train: Optional[
            Union[Collection[PolicyID], Callable[[PolicyID, SampleBatchType], bool]]
        ] = NotProvided,
        policy_states_are_swappable: Optional[bool] = NotProvided,
        observation_fn: Optional[Callable] = NotProvided,
        count_steps_by: Optional[str] = NotProvided,
        # Deprecated args:
        algorithm_config_overrides_per_module=DEPRECATED_VALUE,
        replay_mode=DEPRECATED_VALUE,
        # Now done via Ray object store, which has its own cloud-supported
        # spillover mechanism.
        policy_map_cache=DEPRECATED_VALUE,
    ) -> "AlgorithmConfig":
        """Sets the config's multi-agent settings.

        Validates the new multi-agent settings and translates everything into
        a unified multi-agent setup format. For example a `policies` list or set
        of IDs is properly converted into a dict mapping these IDs to PolicySpecs.

        Args:
            policies: Map of type MultiAgentPolicyConfigDict from policy ids to either
                4-tuples of (policy_cls, obs_space, act_space, config) or PolicySpecs.
                These tuples or PolicySpecs define the class of the policy, the
                observation- and action spaces of the policies, and any extra config.
            policy_map_capacity: Keep this many policies in the "policy_map" (before
                writing least-recently used ones to disk/S3).
            policy_mapping_fn: Function mapping agent ids to policy ids. The signature
                is: `(agent_id, episode, worker, **kwargs) -> PolicyID`.
            policies_to_train: Determines those policies that should be updated.
                Options are:
                - None, for training all policies.
                - An iterable of PolicyIDs that should be trained.
                - A callable, taking a PolicyID and a SampleBatch or MultiAgentBatch
                and returning a bool (indicating whether the given policy is trainable
                or not, given the particular batch). This allows you to have a policy
                trained only on certain data (e.g. when playing against a certain
                opponent).
            policy_states_are_swappable: Whether all Policy objects in this map can be
                "swapped out" via a simple `state = A.get_state(); B.set_state(state)`,
                where `A` and `B` are policy instances in this map. You should set
                this to True for significantly speeding up the PolicyMap's cache lookup
                times, iff your policies all share the same neural network
                architecture and optimizer types. If True, the PolicyMap will not
                have to garbage collect old, least recently used policies, but instead
                keep them in memory and simply override their state with the state of
                the most recently accessed one.
                For example, in a league-based training setup, you might have 100s of
                the same policies in your map (playing against each other in various
                combinations), but all of them share the same state structure
                (are "swappable").
            observation_fn: Optional function that can be used to enhance the local
                agent observations to include more state. See
                rllib/evaluation/observation_function.py for more info.
            count_steps_by: Which metric to use as the "batch size" when building a
                MultiAgentBatch. The two supported values are:
                "env_steps": Count each time the env is "stepped" (no matter how many
                multi-agent actions are passed/how many multi-agent observations
                have been returned in the previous step).
                "agent_steps": Count each individual agent step as one step.

        Returns:
            This updated AlgorithmConfig object.
        """
        if policies is not NotProvided:
            # Make sure our Policy IDs are ok (this should work whether `policies`
            # is a dict or just any Sequence).
            for pid in policies:
                validate_module_id(pid, error=True)

            # Collection: Convert to dict.
            if isinstance(policies, (set, tuple, list)):
                policies = {p: PolicySpec() for p in policies}
            # Validate each policy spec in a given dict.
            if isinstance(policies, dict):
                for pid, spec in policies.items():
                    # If not a PolicySpec object, values must be lists/tuples of len 4.
                    if not isinstance(spec, PolicySpec):
                        if not isinstance(spec, (list, tuple)) or len(spec) != 4:
                            raise ValueError(
                                "Policy specs must be tuples/lists of "
                                "(cls or None, obs_space, action_space, config), "
                                f"got {spec} for PolicyID={pid}"
                            )
                    # TODO: Switch from dict to AlgorithmConfigOverride, once available.
                    # Config not a dict.
                    elif (
                        not isinstance(spec.config, (AlgorithmConfig, dict))
                        and spec.config is not None
                    ):
                        raise ValueError(
                            f"Multi-agent policy config for {pid} must be a dict or "
                            f"AlgorithmConfig object, but got {type(spec.config)}!"
                        )
                self.policies = policies
            else:
                raise ValueError(
                    "`policies` must be dict mapping PolicyID to PolicySpec OR a "
                    "set/tuple/list of PolicyIDs!"
                )

        if algorithm_config_overrides_per_module != DEPRECATED_VALUE:
            deprecation_warning(old="", error=False)
            self.rl_module(
                algorithm_config_overrides_per_module=(
                    algorithm_config_overrides_per_module
                )
            )

        if policy_map_capacity is not NotProvided:
            self.policy_map_capacity = policy_map_capacity

        if policy_mapping_fn is not NotProvided:
            # Create `policy_mapping_fn` from a config dict.
            # Helpful if users would like to specify custom callable classes in
            # yaml files.
            if isinstance(policy_mapping_fn, dict):
                policy_mapping_fn = from_config(policy_mapping_fn)
            self.policy_mapping_fn = policy_mapping_fn

        if observation_fn is not NotProvided:
            self.observation_fn = observation_fn

        if policy_map_cache != DEPRECATED_VALUE:
            deprecation_warning(
                old="AlgorithmConfig.multi_agent(policy_map_cache=..)",
                error=True,
            )

        if replay_mode != DEPRECATED_VALUE:
            deprecation_warning(
                old="AlgorithmConfig.multi_agent(replay_mode=..)",
                new="AlgorithmConfig.training("
                "replay_buffer_config={'replay_mode': ..})",
                error=True,
            )

        if count_steps_by is not NotProvided:
            if count_steps_by not in ["env_steps", "agent_steps"]:
                raise ValueError(
                    "config.multi_agent(count_steps_by=..) must be one of "
                    f"[env_steps|agent_steps], not {count_steps_by}!"
                )
            self.count_steps_by = count_steps_by

        if policies_to_train is not NotProvided:
            assert (
                isinstance(policies_to_train, (list, set, tuple))
                or callable(policies_to_train)
                or policies_to_train is None
            ), (
                "ERROR: `policies_to_train` must be a [list|set|tuple] or a "
                "callable taking PolicyID and SampleBatch and returning "
                "True|False (trainable or not?) or None (for always training all "
                "policies)."
            )
            # Check `policies_to_train` for invalid entries.
            if isinstance(policies_to_train, (list, set, tuple)):
                if len(policies_to_train) == 0:
                    logger.warning(
                        "`config.multi_agent(policies_to_train=..)` is empty! "
                        "Make sure - if you would like to learn at least one policy - "
                        "to add its ID to that list."
                    )
            self.policies_to_train = policies_to_train

        if policy_states_are_swappable is not NotProvided:
            self.policy_states_are_swappable = policy_states_are_swappable

        return self

    def is_multi_agent(self) -> bool:
        """Returns whether this config specifies a multi-agent setup.

        Returns:
            True, if a) >1 policies defined OR b) 1 policy defined, but its ID is NOT
            DEFAULT_POLICY_ID.
        """
        return len(self.policies) > 1 or DEFAULT_POLICY_ID not in self.policies

    def reporting(
        self,
        *,
        keep_per_episode_custom_metrics: Optional[bool] = NotProvided,
        metrics_episode_collection_timeout_s: Optional[float] = NotProvided,
        metrics_num_episodes_for_smoothing: Optional[int] = NotProvided,
        min_time_s_per_iteration: Optional[float] = NotProvided,
        min_train_timesteps_per_iteration: Optional[int] = NotProvided,
        min_sample_timesteps_per_iteration: Optional[int] = NotProvided,
        log_gradients: Optional[bool] = NotProvided,
    ) -> "AlgorithmConfig":
        """Sets the config's reporting settings.

        Args:
            keep_per_episode_custom_metrics: Store raw custom metrics without
                calculating max, min, mean
            metrics_episode_collection_timeout_s: Wait for metric batches for at most
                this many seconds. Those that have not returned in time will be
                collected in the next train iteration.
            metrics_num_episodes_for_smoothing: Smooth rollout metrics over this many
                episodes, if possible.
                In case rollouts (sample collection) just started, there may be fewer
                than this many episodes in the buffer and we'll compute metrics
                over this smaller number of available episodes.
                In case there are more than this many episodes collected in a single
                training iteration, use all of these episodes for metrics computation,
                meaning don't ever cut any "excess" episodes.
                Set this to 1 to disable smoothing and to always report only the most
                recently collected episode's return.
            min_time_s_per_iteration: Minimum time (in sec) to accumulate within a
                single `Algorithm.train()` call. This value does not affect learning,
                only the number of times `Algorithm.training_step()` is called by
                `Algorithm.train()`. If - after one such step attempt, the time taken
                has not reached `min_time_s_per_iteration`, will perform n more
                `Algorithm.training_step()` calls until the minimum time has been
                consumed. Set to 0 or None for no minimum time.
            min_train_timesteps_per_iteration: Minimum training timesteps to accumulate
                within a single `train()` call. This value does not affect learning,
                only the number of times `Algorithm.training_step()` is called by
                `Algorithm.train()`. If - after one such step attempt, the training
                timestep count has not been reached, will perform n more
                `training_step()` calls until the minimum timesteps have been
                executed. Set to 0 or None for no minimum timesteps.
            min_sample_timesteps_per_iteration: Minimum env sampling timesteps to
                accumulate within a single `train()` call. This value does not affect
                learning, only the number of times `Algorithm.training_step()` is
                called by `Algorithm.train()`. If - after one such step attempt, the env
                sampling timestep count has not been reached, will perform n more
                `training_step()` calls until the minimum timesteps have been
                executed. Set to 0 or None for no minimum timesteps.
            log_gradients: Log gradients to results. If this is `True` the global norm
                of the gradients dictionariy for each optimizer is logged to results.
                The default is `True`.

        Returns:
            This updated AlgorithmConfig object.
        """
        if keep_per_episode_custom_metrics is not NotProvided:
            self.keep_per_episode_custom_metrics = keep_per_episode_custom_metrics
        if metrics_episode_collection_timeout_s is not NotProvided:
            self.metrics_episode_collection_timeout_s = (
                metrics_episode_collection_timeout_s
            )
        if metrics_num_episodes_for_smoothing is not NotProvided:
            self.metrics_num_episodes_for_smoothing = metrics_num_episodes_for_smoothing
        if min_time_s_per_iteration is not NotProvided:
            self.min_time_s_per_iteration = min_time_s_per_iteration
        if min_train_timesteps_per_iteration is not NotProvided:
            self.min_train_timesteps_per_iteration = min_train_timesteps_per_iteration
        if min_sample_timesteps_per_iteration is not NotProvided:
            self.min_sample_timesteps_per_iteration = min_sample_timesteps_per_iteration
        if log_gradients is not NotProvided:
            self.log_gradients = log_gradients

        return self

    def checkpointing(
        self,
        export_native_model_files: Optional[bool] = NotProvided,
        checkpoint_trainable_policies_only: Optional[bool] = NotProvided,
    ) -> "AlgorithmConfig":
        """Sets the config's checkpointing settings.

        Args:
            export_native_model_files: Whether an individual Policy-
                or the Algorithm's checkpoints also contain (tf or torch) native
                model files. These could be used to restore just the NN models
                from these files w/o requiring RLlib. These files are generated
                by calling the tf- or torch- built-in saving utility methods on
                the actual models.
            checkpoint_trainable_policies_only: Whether to only add Policies to the
                Algorithm checkpoint (in sub-directory "policies/") that are trainable
                according to the `is_trainable_policy` callable of the local worker.

        Returns:
            This updated AlgorithmConfig object.
        """

        if export_native_model_files is not NotProvided:
            self.export_native_model_files = export_native_model_files
        if checkpoint_trainable_policies_only is not NotProvided:
            self.checkpoint_trainable_policies_only = checkpoint_trainable_policies_only

        return self

    def debugging(
        self,
        *,
        logger_creator: Optional[Callable[[], Logger]] = NotProvided,
        logger_config: Optional[dict] = NotProvided,
        log_level: Optional[str] = NotProvided,
        log_sys_usage: Optional[bool] = NotProvided,
        fake_sampler: Optional[bool] = NotProvided,
        seed: Optional[int] = NotProvided,
        _run_training_always_in_thread: Optional[bool] = NotProvided,
        _evaluation_parallel_to_training_wo_thread: Optional[bool] = NotProvided,
    ) -> "AlgorithmConfig":
        """Sets the config's debugging settings.

        Args:
            logger_creator: Callable that creates a ray.tune.Logger
                object. If unspecified, a default logger is created.
            logger_config: Define logger-specific configuration to be used inside Logger
                Default value None allows overwriting with nested dicts.
            log_level: Set the ray.rllib.* log level for the agent process and its
                workers. Should be one of DEBUG, INFO, WARN, or ERROR. The DEBUG level
                will also periodically print out summaries of relevant internal dataflow
                (this is also printed out once at startup at the INFO level).
            log_sys_usage: Log system resource metrics to results. This requires
                `psutil` to be installed for sys stats, and `gputil` for GPU metrics.
            fake_sampler: Use fake (infinite speed) sampler. For testing only.
            seed: This argument, in conjunction with worker_index, sets the random
                seed of each worker, so that identically configured trials will have
                identical results. This makes experiments reproducible.
            _run_training_always_in_thread: Runs the n `training_step()` calls per
                iteration always in a separate thread (just as we would do with
                `evaluation_parallel_to_training=True`, but even without evaluation
                going on and even without evaluation workers being created in the
                Algorithm).
            _evaluation_parallel_to_training_wo_thread: Only relevant if
                `evaluation_parallel_to_training` is True. Then, in order to achieve
                parallelism, RLlib will not use a thread pool (as it usually does in
                this situation).

        Returns:
            This updated AlgorithmConfig object.
        """
        if logger_creator is not NotProvided:
            self.logger_creator = logger_creator
        if logger_config is not NotProvided:
            self.logger_config = logger_config
        if log_level is not NotProvided:
            self.log_level = log_level
        if log_sys_usage is not NotProvided:
            self.log_sys_usage = log_sys_usage
        if fake_sampler is not NotProvided:
            self.fake_sampler = fake_sampler
        if seed is not NotProvided:
            self.seed = seed
        if _run_training_always_in_thread is not NotProvided:
            self._run_training_always_in_thread = _run_training_always_in_thread
        if _evaluation_parallel_to_training_wo_thread is not NotProvided:
            self._evaluation_parallel_to_training_wo_thread = (
                _evaluation_parallel_to_training_wo_thread
            )

        return self

    def fault_tolerance(
        self,
        ignore_env_runner_failures: Optional[bool] = NotProvided,
        recreate_failed_env_runners: Optional[bool] = NotProvided,
        max_num_env_runner_restarts: Optional[int] = NotProvided,
        delay_between_env_runner_restarts_s: Optional[float] = NotProvided,
        restart_failed_sub_environments: Optional[bool] = NotProvided,
        num_consecutive_env_runner_failures_tolerance: Optional[int] = NotProvided,
        env_runner_health_probe_timeout_s: Optional[float] = NotProvided,
        env_runner_restore_timeout_s: Optional[float] = NotProvided,
        # Deprecated args.
        ignore_worker_failures=DEPRECATED_VALUE,
        recreate_failed_workers=DEPRECATED_VALUE,
        max_num_worker_restarts=DEPRECATED_VALUE,
        delay_between_worker_restarts_s=DEPRECATED_VALUE,
        num_consecutive_worker_failures_tolerance=DEPRECATED_VALUE,
        worker_health_probe_timeout_s=DEPRECATED_VALUE,
        worker_restore_timeout_s=DEPRECATED_VALUE,
    ):
        """Sets the config's fault tolerance settings.

        Args:
            ignore_env_runner_failures: Whether to ignore any EnvRunner failures
                and continue running with the remaining EnvRunners. This setting will
                be ignored, if `recreate_failed_env_runners=True`.
            recreate_failed_env_runners: Whether - upon an EnvRunner failure - RLlib
                will try to recreate the lost EnvRunner as an identical copy of the
                failed one. The new EnvRunner will only differ from the failed one in
                its `self.recreated_worker=True` property value. It will have the same
                `worker_index` as the original one. If True, the
                `ignore_env_runner_failures` setting will be ignored.
            max_num_env_runner_restarts: The maximum number of times any EnvRunner
                is allowed to be restarted (if `recreate_failed_env_runners` is True).
            delay_between_env_runner_restarts_s: The delay (in seconds) between two
                consecutive EnvRunner restarts (if `recreate_failed_env_runners` is
                True).
            restart_failed_sub_environments: If True and any sub-environment (within
                a vectorized env) throws any error during env stepping, the
                Sampler will try to restart the faulty sub-environment. This is done
                without disturbing the other (still intact) sub-environment and without
                the EnvRunner crashing.
            num_consecutive_env_runner_failures_tolerance: The number of consecutive
                times an EnvRunner failure (also for evaluation) is tolerated before
                finally crashing the Algorithm. Only useful if either
                `ignore_env_runner_failures` or `recreate_failed_env_runners` is True.
                Note that for `restart_failed_sub_environments` and sub-environment
                failures, the EnvRunner itself is NOT affected and won't throw any
                errors as the flawed sub-environment is silently restarted under the
                hood.
            env_runner_health_probe_timeout_s: Max amount of time in seconds, we should
                spend waiting for EnvRunner health probe calls
                (`EnvRunner.ping.remote()`) to respond. Health pings are very cheap,
                however, we perform the health check via a blocking `ray.get()`, so the
                default value should not be too large.
            env_runner_restore_timeout_s: Max amount of time we should wait to restore
                states on recovered EnvRunner actors. Default is 30 mins.

        Returns:
            This updated AlgorithmConfig object.
        """
        if ignore_worker_failures != DEPRECATED_VALUE:
            deprecation_warning(
                old="AlgorithmConfig.fault_tolerance(ignore_worker_failures)",
                new="AlgorithmConfig.fault_tolerance(ignore_env_runner_failures)",
                error=True,
            )
        if recreate_failed_workers != DEPRECATED_VALUE:
            deprecation_warning(
                old="AlgorithmConfig.fault_tolerance(recreate_failed_workers)",
                new="AlgorithmConfig.fault_tolerance(recreate_failed_env_runners)",
                error=True,
            )
        if max_num_worker_restarts != DEPRECATED_VALUE:
            deprecation_warning(
                old="AlgorithmConfig.fault_tolerance(max_num_worker_restarts)",
                new="AlgorithmConfig.fault_tolerance(max_num_env_runner_restarts)",
                error=True,
            )
        if delay_between_worker_restarts_s != DEPRECATED_VALUE:
            deprecation_warning(
                old="AlgorithmConfig.fault_tolerance(delay_between_worker_restarts_s)",
                new="AlgorithmConfig.fault_tolerance(delay_between_env_runner_"
                "restarts_s)",
                error=True,
            )
        if num_consecutive_worker_failures_tolerance != DEPRECATED_VALUE:
            deprecation_warning(
                old="AlgorithmConfig.fault_tolerance(num_consecutive_worker_"
                "failures_tolerance)",
                new="AlgorithmConfig.fault_tolerance(num_consecutive_env_runner_"
                "failures_tolerance)",
                error=True,
            )
        if worker_health_probe_timeout_s != DEPRECATED_VALUE:
            deprecation_warning(
                old="AlgorithmConfig.fault_tolerance(worker_health_probe_timeout_s)",
                new="AlgorithmConfig.fault_tolerance("
                "env_runner_health_probe_timeout_s)",
                error=True,
            )
        if worker_restore_timeout_s != DEPRECATED_VALUE:
            deprecation_warning(
                old="AlgorithmConfig.fault_tolerance(worker_restore_timeout_s)",
                new="AlgorithmConfig.fault_tolerance(env_runner_restore_timeout_s)",
                error=True,
            )

        if ignore_env_runner_failures is not NotProvided:
            self.ignore_env_runner_failures = ignore_env_runner_failures
        if recreate_failed_env_runners is not NotProvided:
            self.recreate_failed_env_runners = recreate_failed_env_runners
        if max_num_env_runner_restarts is not NotProvided:
            self.max_num_env_runner_restarts = max_num_env_runner_restarts
        if delay_between_env_runner_restarts_s is not NotProvided:
            self.delay_between_env_runner_restarts_s = (
                delay_between_env_runner_restarts_s
            )
        if restart_failed_sub_environments is not NotProvided:
            self.restart_failed_sub_environments = restart_failed_sub_environments
        if num_consecutive_env_runner_failures_tolerance is not NotProvided:
            self.num_consecutive_env_runner_failures_tolerance = (
                num_consecutive_env_runner_failures_tolerance
            )
        if env_runner_health_probe_timeout_s is not NotProvided:
            self.env_runner_health_probe_timeout_s = env_runner_health_probe_timeout_s
        if env_runner_restore_timeout_s is not NotProvided:
            self.env_runner_restore_timeout_s = env_runner_restore_timeout_s

        return self

    def rl_module(
        self,
        *,
        model_config_dict: Optional[Dict[str, Any]] = NotProvided,
        rl_module_spec: Optional[RLModuleSpecType] = NotProvided,
        algorithm_config_overrides_per_module: Optional[
            Dict[ModuleID, PartialAlgorithmConfigDict]
        ] = NotProvided,
        # Deprecated arg.
        _enable_rl_module_api=DEPRECATED_VALUE,
    ) -> "AlgorithmConfig":
        """Sets the config's RLModule settings.

        Args:
            model_config_dict: The default model config dictionary for `RLModule`s. This
                will be used for any `RLModule` if not otherwise specified in the
                `rl_module_spec`.
            rl_module_spec: The RLModule spec to use for this config. It can be either
                a RLModuleSpec or a MultiRLModuleSpec. If the
                observation_space, action_space, catalog_class, or the model config is
                not specified it will be inferred from the env and other parts of the
                algorithm config object.
            algorithm_config_overrides_per_module: Only used if
                `enable_rl_module_and_learner=True`.
                A mapping from ModuleIDs to per-module AlgorithmConfig override dicts,
                which apply certain settings,
                e.g. the learning rate, from the main AlgorithmConfig only to this
                particular module (within a MultiRLModule).
                You can create override dicts by using the `AlgorithmConfig.overrides`
                utility. For example, to override your learning rate and (PPO) lambda
                setting just for a single RLModule with your MultiRLModule, do:
                config.multi_agent(algorithm_config_overrides_per_module={
                "module_1": PPOConfig.overrides(lr=0.0002, lambda_=0.75),
                })

        Returns:
            This updated AlgorithmConfig object.
        """
        if _enable_rl_module_api != DEPRECATED_VALUE:
            deprecation_warning(
                old="AlgorithmConfig.rl_module(_enable_rl_module_api=..)",
                new="AlgorithmConfig.api_stack(enable_rl_module_and_learner=..)",
                error=True,
            )

        if model_config_dict is not NotProvided:
            self._model_config_dict = model_config_dict
        if rl_module_spec is not NotProvided:
            self._rl_module_spec = rl_module_spec
        if algorithm_config_overrides_per_module is not NotProvided:
            if not isinstance(algorithm_config_overrides_per_module, dict):
                raise ValueError(
                    "`algorithm_config_overrides_per_module` must be a dict mapping "
                    "module IDs to config override dicts! You provided "
                    f"{algorithm_config_overrides_per_module}."
                )
            self.algorithm_config_overrides_per_module.update(
                algorithm_config_overrides_per_module
            )

        return self

    def experimental(
        self,
        *,
        _torch_grad_scaler_class: Optional[Type] = NotProvided,
        _torch_lr_scheduler_classes: Optional[
            Union[List[Type], Dict[ModuleID, Type]]
        ] = NotProvided,
        _tf_policy_handles_more_than_one_loss: Optional[bool] = NotProvided,
        _disable_preprocessor_api: Optional[bool] = NotProvided,
        _disable_action_flattening: Optional[bool] = NotProvided,
        _disable_initialize_loss_from_dummy_batch: Optional[bool] = NotProvided,
        # Deprecated args.
        _enable_new_api_stack=DEPRECATED_VALUE,
    ) -> "AlgorithmConfig":
        """Sets the config's experimental settings.

        Args:
            _torch_grad_scaler_class: Class to use for torch loss scaling (and gradient
                unscaling). The class must implement the following methods to be
                compatible with a `TorchLearner`. These methods/APIs match exactly those
                of torch's own `torch.amp.GradScaler` (see here for more details
                https://pytorch.org/docs/stable/amp.html#gradient-scaling):
                `scale([loss])` to scale the loss by some factor.
                `get_scale()` to get the current scale factor value.
                `step([optimizer])` to unscale the grads (divide by the scale factor)
                and step the given optimizer.
                `update()` to update the scaler after an optimizer step (for example to
                adjust the scale factor).
            _torch_lr_scheduler_classes: A list of `torch.lr_scheduler.LRScheduler`
                (see here for more details
                https://pytorch.org/docs/stable/optim.html#how-to-adjust-learning-rate)
                classes or a dictionary mapping module IDs to such a list of respective
                scheduler classes. Multiple scheduler classes can be applied in sequence
                and will be stepped in the same sequence as defined here. Note, most
                learning rate schedulers need arguments to be configured, i.e. you need
                to partially initialize the schedulers in the list(s).
            _tf_policy_handles_more_than_one_loss: Experimental flag.
                If True, TFPolicy will handle more than one loss/optimizer.
                Set this to True, if you would like to return more than
                one loss term from your `loss_fn` and an equal number of optimizers
                from your `optimizer_fn`. In the future, the default for this will be
                True.
            _disable_preprocessor_api: Experimental flag.
                If True, no (observation) preprocessor will be created and
                observations will arrive in model as they are returned by the env.
                In the future, the default for this will be True.
            _disable_action_flattening: Experimental flag.
                If True, RLlib will no longer flatten the policy-computed actions into
                a single tensor (for storage in SampleCollectors/output files/etc..),
                but leave (possibly nested) actions as-is. Disabling flattening affects:
                - SampleCollectors: Have to store possibly nested action structs.
                - Models that have the previous action(s) as part of their input.
                - Algorithms reading from offline files (incl. action information).

        Returns:
            This updated AlgorithmConfig object.
        """
        if _enable_new_api_stack != DEPRECATED_VALUE:
            deprecation_warning(
                old="config.experimental(_enable_new_api_stack=...)",
                new="config.api_stack(enable_rl_module_and_learner=...)",
                error=False,
            )
            self.api_stack(enable_rl_module_and_learner=_enable_new_api_stack)

        if _tf_policy_handles_more_than_one_loss is not NotProvided:
            self._tf_policy_handles_more_than_one_loss = (
                _tf_policy_handles_more_than_one_loss
            )
        if _disable_preprocessor_api is not NotProvided:
            self._disable_preprocessor_api = _disable_preprocessor_api
        if _disable_action_flattening is not NotProvided:
            self._disable_action_flattening = _disable_action_flattening
        if _disable_initialize_loss_from_dummy_batch is not NotProvided:
            self._disable_initialize_loss_from_dummy_batch = (
                _disable_initialize_loss_from_dummy_batch
            )
        if _torch_grad_scaler_class is not NotProvided:
            self._torch_grad_scaler_class = _torch_grad_scaler_class
        if _torch_lr_scheduler_classes is not NotProvided:
            self._torch_lr_scheduler_classes = _torch_lr_scheduler_classes

        return self

    @property
    def rl_module_spec(self):
        default_rl_module_spec = self.get_default_rl_module_spec()
        _check_rl_module_spec(default_rl_module_spec)

        # `self._rl_module_spec` has been user defined (via call to `self.rl_module()`).
        if self._rl_module_spec is not None:
            # Merge provided RL Module spec class with defaults
            _check_rl_module_spec(self._rl_module_spec)
            # Merge given spec with default one (in case items are missing, such as
            # spaces, module class, etc.)
            if isinstance(self._rl_module_spec, RLModuleSpec):
                if isinstance(default_rl_module_spec, RLModuleSpec):
                    default_rl_module_spec.update(self._rl_module_spec)
                    return default_rl_module_spec
                elif isinstance(default_rl_module_spec, MultiRLModuleSpec):
                    raise ValueError(
                        "Cannot merge MultiRLModuleSpec with RLModuleSpec!"
                    )
            else:
                multi_rl_module_spec = copy.deepcopy(self._rl_module_spec)
                multi_rl_module_spec.update(default_rl_module_spec)
                return multi_rl_module_spec

        # `self._rl_module_spec` has not been user defined -> return default one.
        else:
            return default_rl_module_spec

    @property
    def learner_class(self) -> Type["Learner"]:
        """Returns the Learner sub-class to use by this Algorithm.

        Either
        a) User sets a specific learner class via calling `.training(learner_class=...)`
        b) User leaves learner class unset (None) and the AlgorithmConfig itself
        figures out the actual learner class by calling its own
        `.get_default_learner_class()` method.
        """
        return self._learner_class or self.get_default_learner_class()

    @property
    def is_atari(self) -> bool:
        """True if if specified env is an Atari env."""

        # Not yet determined, try to figure this out.
        if self._is_atari is None:
            # Atari envs are usually specified via a string like "PongNoFrameskip-v4"
            # or "ALE/Breakout-v5".
            # We do NOT attempt to auto-detect Atari env for other specified types like
            # a callable, to avoid running heavy logics in validate().
            # For these cases, users can explicitly set `environment(atari=True)`.
            if type(self.env) is not str:
                return False
            try:
                env = gym.make(self.env)
            # Any gymnasium error -> Cannot be an Atari env.
            except gym.error.Error:
                return False

            self._is_atari = is_atari(env)
            # Clean up env's resources, if any.
            env.close()

        return self._is_atari

    @property
    def total_train_batch_size(self):
        if (
            self.train_batch_size_per_learner is not None
            and self.enable_rl_module_and_learner
        ):
            return self.train_batch_size_per_learner * (self.num_learners or 1)
        else:
            return self.train_batch_size

    # TODO: Make rollout_fragment_length as read-only property and replace the current
    #  self.rollout_fragment_length a private variable.
    def get_rollout_fragment_length(self, worker_index: int = 0) -> int:
        """Automatically infers a proper rollout_fragment_length setting if "auto".

        Uses the simple formula:
        `rollout_fragment_length` = `total_train_batch_size` /
        (`num_envs_per_env_runner` * `num_env_runners`)

        If result is a fraction AND `worker_index` is provided, will make
        those workers add additional timesteps, such that the overall batch size (across
        the workers) will add up to exactly the `total_train_batch_size`.

        Returns:
            The user-provided `rollout_fragment_length` or a computed one (if user
            provided value is "auto"), making sure `total_train_batch_size` is reached
            exactly in each iteration.
        """
        if self.rollout_fragment_length == "auto":
            # Example:
            # 2 workers, 2 envs per worker, 2000 train batch size:
            # -> 2000 / 4 -> 500
            # 4 workers, 3 envs per worker, 2500 train batch size:
            # -> 2500 / 12 -> 208.333 -> diff=4 (208 * 12 = 2496)
            # -> worker 1, 2: 209, workers 3, 4: 208
            # 2 workers, 20 envs per worker, 512 train batch size:
            # -> 512 / 40 -> 12.8 -> diff=32 (12 * 40 = 480)
            # -> worker 1: 13, workers 2: 12
            rollout_fragment_length = self.total_train_batch_size / (
                self.num_envs_per_env_runner * (self.num_env_runners or 1)
            )
            if int(rollout_fragment_length) != rollout_fragment_length:
                diff = self.total_train_batch_size - int(
                    rollout_fragment_length
                ) * self.num_envs_per_env_runner * (self.num_env_runners or 1)
                if ((worker_index - 1) * self.num_envs_per_env_runner) >= diff:
                    return int(rollout_fragment_length)
                else:
                    return int(rollout_fragment_length) + 1
            return int(rollout_fragment_length)
        else:
            return self.rollout_fragment_length

    # TODO: Make evaluation_config as read-only property and replace the current
    #  self.evaluation_config a private variable.
    def get_evaluation_config_object(
        self,
    ) -> Optional["AlgorithmConfig"]:
        """Creates a full AlgorithmConfig object from `self.evaluation_config`.

        Returns:
            A fully valid AlgorithmConfig object that can be used for the evaluation
            EnvRunnerGroup. If `self` is already an evaluation config object, return
            None.
        """
        if self.in_evaluation:
            assert self.evaluation_config is None
            return None

        evaluation_config = self.evaluation_config
        # Already an AlgorithmConfig -> copy and use as-is.
        if isinstance(evaluation_config, AlgorithmConfig):
            eval_config_obj = evaluation_config.copy(copy_frozen=False)
        # Create unfrozen copy of self to be used as the to-be-returned eval
        # AlgorithmConfig.
        else:
            eval_config_obj = self.copy(copy_frozen=False)
            # Update with evaluation override settings:
            eval_config_obj.update_from_dict(evaluation_config or {})

        # Switch on the `in_evaluation` flag and remove `evaluation_config`
        # (set to None).
        eval_config_obj.in_evaluation = True
        eval_config_obj.evaluation_config = None

        # NOTE: The following if-block is only relevant for the old API stack.
        # For the new API stack (EnvRunners), the evaluation methods of Algorithm
        # explicitly tell each EnvRunner on each sample call, how many timesteps
        # of episodes to collect.
        # Evaluation duration unit: episodes.
        # Switch on `complete_episode` rollouts. Also, make sure
        # rollout fragments are short so we never have more than one
        # episode in one rollout.
        if self.evaluation_duration_unit == "episodes":
            eval_config_obj.batch_mode = "complete_episodes"
            eval_config_obj.rollout_fragment_length = 1
        # Evaluation duration unit: timesteps.
        # - Set `batch_mode=truncate_episodes` so we don't perform rollouts
        #   strictly along episode borders.
        # Set `rollout_fragment_length` such that desired steps are divided
        # equally amongst workers or - in "auto" duration mode - set it
        # to a reasonably small number (10), such that a single `sample()`
        # call doesn't take too much time and we can stop evaluation as soon
        # as possible after the train step is completed.
        else:
            eval_config_obj.batch_mode = "truncate_episodes"
            eval_config_obj.rollout_fragment_length = (
                # Set to a moderately small (but not too small) value in order
                # to a) not overshoot too much the parallelly running `training_step`
                # but also to b) avoid too many `sample()` remote calls.
                # 100 seems like a good middle ground.
                100
                if self.evaluation_duration == "auto"
                else int(
                    math.ceil(
                        self.evaluation_duration
                        / (self.evaluation_num_env_runners or 1)
                    )
                )
            )

        return eval_config_obj

    # TODO: Move this to those algorithms that really need this, which is currently
    #  only A2C and PG.
    def validate_train_batch_size_vs_rollout_fragment_length(self) -> None:
        """Detects mismatches for `train_batch_size` vs `rollout_fragment_length`.

        Only applicable for algorithms, whose train_batch_size should be directly
        dependent on rollout_fragment_length (synchronous sampling, on-policy PG algos).

        If rollout_fragment_length != "auto", makes sure that the product of
        `rollout_fragment_length` x `num_env_runners` x `num_envs_per_env_runner`
        roughly (10%) matches the provided `train_batch_size`. Otherwise, errors with
        asking the user to set rollout_fragment_length to `auto` or to a matching
        value.

        Also, only checks this if `train_batch_size` > 0 (DDPPO sets this
        to -1 to auto-calculate the actual batch size later).

        Raises:
            ValueError: If there is a mismatch between user provided
                `rollout_fragment_length` and `total_train_batch_size`.
        """
        if (
            self.rollout_fragment_length != "auto"
            and not self.in_evaluation
            and self.total_train_batch_size > 0
        ):
            min_batch_size = (
                max(self.num_env_runners, 1)
                * self.num_envs_per_env_runner
                * self.rollout_fragment_length
            )
            batch_size = min_batch_size
            while batch_size < self.total_train_batch_size:
                batch_size += min_batch_size
            if batch_size - self.total_train_batch_size > (
                0.1 * self.total_train_batch_size
            ) or batch_size - min_batch_size - self.total_train_batch_size > (
                0.1 * self.total_train_batch_size
            ):
                suggested_rollout_fragment_length = self.total_train_batch_size // (
                    self.num_envs_per_env_runner * (self.num_env_runners or 1)
                )
                raise ValueError(
                    "Your desired `total_train_batch_size` "
                    f"({self.total_train_batch_size}={self.num_learners} "
                    f"learners x {self.train_batch_size_per_learner}) "
                    "or a value 10% off of that cannot be achieved with your other "
                    f"settings (num_env_runners={self.num_env_runners}; "
                    f"num_envs_per_env_runner={self.num_envs_per_env_runner}; "
                    f"rollout_fragment_length={self.rollout_fragment_length})! "
                    "Try setting `rollout_fragment_length` to 'auto' OR to a value of "
                    f"{suggested_rollout_fragment_length}."
                )

    def get_torch_compile_worker_config(self):
        """Returns the TorchCompileConfig to use on workers."""

        from ray.rllib.core.rl_module.torch.torch_compile_config import (
            TorchCompileConfig,
        )

        return TorchCompileConfig(
            torch_dynamo_backend=self.torch_compile_worker_dynamo_backend,
            torch_dynamo_mode=self.torch_compile_worker_dynamo_mode,
        )

    def get_default_rl_module_spec(self) -> RLModuleSpecType:
        """Returns the RLModule spec to use for this algorithm.

        Override this method in the subclass to return the RLModule spec, given
        the input framework.

        Returns:
            The RLModuleSpec (or MultiRLModuleSpec) to
            use for this algorithm's RLModule.
        """
        raise NotImplementedError

    def get_default_learner_class(self) -> Union[Type["Learner"], str]:
        """Returns the Learner class to use for this algorithm.

        Override this method in the sub-class to return the Learner class type given
        the input framework.

        Returns:
            The Learner class to use for this algorithm either as a class type or as
            a string (e.g. ray.rllib.core.learner.testing.torch.BC).
        """
        raise NotImplementedError

    def get_rl_module_spec(
        self,
        env: Optional[EnvType] = None,
        spaces: Optional[Dict[str, gym.Space]] = None,
        inference_only: Optional[bool] = None,
    ) -> RLModuleSpec:
        """Returns the RLModuleSpec based on the given env/spaces.

        Args:
            env: An optional environment instance, from which to infer the observation-
                and action spaces for the RLModule. If not provided, will try to infer
                from `spaces`, otherwise from `self.observation_space` and
                `self.action_space`. If no information on spaces can be inferred, will
                raise an error.
            spaces: Optional dict mapping ModuleIDs to 2-tuples of observation- and
                action space that should be used for the respective RLModule.
                These spaces are usually provided by an already instantiated remote
                EnvRunner (call `EnvRunner.get_spaces()`). If not provided, will try
                to infer from `env`, otherwise from `self.observation_space` and
                `self.action_space`. If no information on spaces can be inferred,
                will raise an error.
            inference_only: If `True`, the returned module spec will be used in an
                inference-only setting (sampling) and the RLModule can thus be built in
                its light version (if available). For example, the `inference_only`
                version of an RLModule might only contain the networks required for
                computing actions, but misses additional target- or critic networks.

        Returns:
            A new RLModuleSpec instance that can be used to build an RLModule.
        """
        rl_module_spec = copy.deepcopy(self.rl_module_spec)

        # If a MultiRLModuleSpec -> Reduce to single-agent (and assert that
        # all non DEFAULT_MODULE_IDs are `learner_only` (so will not be built
        # here on EnvRunner).
        if isinstance(rl_module_spec, MultiRLModuleSpec):
            error = False
            if DEFAULT_MODULE_ID not in rl_module_spec:
                error = True
            if inference_only:
                for mid, spec in rl_module_spec.module_specs.items():
                    if mid != DEFAULT_MODULE_ID:
                        if not spec.learner_only:
                            error = True
            elif len(rl_module_spec) > 1:
                error = True
            if error:
                raise ValueError(
                    "When calling `AlgorithmConfig.get_rl_module_spec()`, the "
                    "configuration must contain the `DEFAULT_MODULE_ID` key and all "
                    "other keys' specs must have the setting `learner_only=True`! If "
                    "you are using a more complex setup, call "
                    "`AlgorithmConfig.get_multi_rl_module_spec(...)` instead."
                )
            rl_module_spec = rl_module_spec[DEFAULT_MODULE_ID]

        if spaces is not None:
            rl_module_spec.observation_space = spaces[DEFAULT_MODULE_ID][0]
            rl_module_spec.action_space = spaces[DEFAULT_MODULE_ID][1]
        elif env is not None:
            if isinstance(env, gym.vector.VectorEnv):
                rl_module_spec.observation_space = env.single_observation_space
            rl_module_spec.action_space = env.single_action_space

        # If module_config_dict is not defined, set to our generic one.
        if rl_module_spec.model_config_dict is None:
            rl_module_spec.model_config_dict = self.model_config

        if inference_only is not None:
            rl_module_spec.inference_only = inference_only

        return rl_module_spec

    def get_multi_rl_module_spec(
        self,
        *,
        env: Optional[EnvType] = None,
        spaces: Optional[Dict[PolicyID, Tuple[Space, Space]]] = None,
        inference_only: bool = False,
        # @HybridAPIStack
        policy_dict: Optional[Dict[str, PolicySpec]] = None,
        single_agent_rl_module_spec: Optional[RLModuleSpec] = None,
    ) -> MultiRLModuleSpec:
        """Returns the MultiRLModuleSpec based on the given env/spaces.

        Args:
            env: An optional environment instance, from which to infer the different
                spaces for the individual RLModules. If not provided, will try to infer
                from `spaces`, otherwise from `self.observation_space` and
                `self.action_space`. If no information on spaces can be inferred, will
                raise an error.
            spaces: Optional dict mapping ModuleIDs to 2-tuples of observation- and
                action space that should be used for the respective RLModule.
                These spaces are usually provided by an already instantiated remote
                EnvRunner (call `EnvRunner.get_spaces()`). If not provided, will try
                to infer from `env`, otherwise from `self.observation_space` and
                `self.action_space`. If no information on spaces can be inferred,
                will raise an error.
            inference_only: If `True`, the returned module spec will be used in an
                inference-only setting (sampling) and the RLModule can thus be built in
                its light version (if available). For example, the `inference_only`
                version of an RLModule might only contain the networks required for
                computing actions, but misses additional target- or critic networks.
                Also, if `True`, the returned spec will NOT contain those (sub)
                RLModuleSpecs that have their `learner_only` flag set to True.

        Returns:
            A new MultiRLModuleSpec instance that can be used to build a MultiRLModule.
        """
        # TODO (Kourosh,sven): When we replace policy entirely there will be no need for
        #  this function to map policy_dict to multi_rl_module_specs anymore. The module
        #  spec will be directly given by the user or inferred from env and spaces.
        if policy_dict is None:
            policy_dict, _ = self.get_multi_agent_setup(env=env, spaces=spaces)

        # TODO (Kourosh): Raise an error if the config is not frozen
        # If the module is single-agent convert it to multi-agent spec

        # The default RLModuleSpec (might be multi-agent or single-agent).
        default_rl_module_spec = self.get_default_rl_module_spec()
        # The currently configured RLModuleSpec (might be multi-agent or single-agent).
        # If None, use the default one.
        current_rl_module_spec = self._rl_module_spec or default_rl_module_spec

        # Algorithm is currently setup as a single-agent one.
        if isinstance(current_rl_module_spec, RLModuleSpec):
            # Use either the provided `single_agent_rl_module_spec` (a
            # RLModuleSpec), the currently configured one of this
            # AlgorithmConfig object, or the default one.
            single_agent_rl_module_spec = (
                single_agent_rl_module_spec or current_rl_module_spec
            )
            single_agent_rl_module_spec.inference_only = inference_only
            # Now construct the proper MultiRLModuleSpec.
            multi_rl_module_spec = MultiRLModuleSpec(
                module_specs={
                    k: copy.deepcopy(single_agent_rl_module_spec)
                    for k in policy_dict.keys()
                },
            )

        # Algorithm is currently setup as a multi-agent one.
        else:
            # The user currently has a MultiAgentSpec setup (either via
            # self._rl_module_spec or the default spec of this AlgorithmConfig).
            assert isinstance(current_rl_module_spec, MultiRLModuleSpec)

            # Default is single-agent but the user has provided a multi-agent spec
            # so the use-case is multi-agent.
            if isinstance(default_rl_module_spec, RLModuleSpec):
                # The individual (single-agent) module specs are defined by the user
                # in the currently setup MultiRLModuleSpec -> Use that
                # RLModuleSpec.
                if isinstance(current_rl_module_spec.module_specs, RLModuleSpec):
                    single_agent_spec = single_agent_rl_module_spec or (
                        current_rl_module_spec.module_specs
                    )
                    single_agent_spec.inference_only = inference_only
                    module_specs = {
                        k: copy.deepcopy(single_agent_spec) for k in policy_dict.keys()
                    }

                # The individual (single-agent) module specs have not been configured
                # via this AlgorithmConfig object -> Use provided single-agent spec or
                # the the default spec (which is also a RLModuleSpec in this
                # case).
                else:
                    single_agent_spec = (
                        single_agent_rl_module_spec or default_rl_module_spec
                    )
                    single_agent_spec.inference_only = inference_only
                    module_specs = {
                        k: copy.deepcopy(
                            current_rl_module_spec.module_specs.get(
                                k, single_agent_spec
                            )
                        )
                        for k in (
                            policy_dict | current_rl_module_spec.module_specs
                        ).keys()
                    }

                # Now construct the proper MultiRLModuleSpec.
                # We need to infer the multi-agent class from `current_rl_module_spec`
                # and fill in the module_specs dict.
                multi_rl_module_spec = current_rl_module_spec.__class__(
                    multi_rl_module_class=current_rl_module_spec.multi_rl_module_class,
                    module_specs=module_specs,
                    modules_to_load=current_rl_module_spec.modules_to_load,
                    load_state_path=current_rl_module_spec.load_state_path,
                )

            # Default is multi-agent and user wants to override it -> Don't use the
            # default.
            else:
                # Use has given an override RLModuleSpec -> Use this to
                # construct the individual RLModules within the MultiRLModuleSpec.
                if single_agent_rl_module_spec is not None:
                    pass
                # User has NOT provided an override RLModuleSpec.
                else:
                    # But the currently setup multi-agent spec has a SingleAgentRLModule
                    # spec defined -> Use that to construct the individual RLModules
                    # within the MultiRLModuleSpec.
                    if isinstance(current_rl_module_spec.module_specs, RLModuleSpec):
                        # The individual module specs are not given, it is given as one
                        # RLModuleSpec to be re-used for all
                        single_agent_rl_module_spec = (
                            current_rl_module_spec.module_specs
                        )
                    # The currently setup multi-agent spec has NO
                    # RLModuleSpec in it -> Error (there is no way we can
                    # infer this information from anywhere at this point).
                    else:
                        raise ValueError(
                            "We have a MultiRLModuleSpec "
                            f"({current_rl_module_spec}), but no "
                            "`RLModuleSpec`s to compile the individual "
                            "RLModules' specs! Use "
                            "`AlgorithmConfig.get_multi_rl_module_spec("
                            "policy_dict=.., single_agent_rl_module_spec=..)`."
                        )

                single_agent_rl_module_spec.inference_only = inference_only

                # Now construct the proper MultiRLModuleSpec.
                multi_rl_module_spec = current_rl_module_spec.__class__(
                    multi_rl_module_class=current_rl_module_spec.multi_rl_module_class,
                    module_specs={
                        k: copy.deepcopy(single_agent_rl_module_spec)
                        for k in policy_dict.keys()
                    },
                    modules_to_load=current_rl_module_spec.modules_to_load,
                    load_state_path=current_rl_module_spec.load_state_path,
                )

        # Fill in the missing values from the specs that we already have. By combining
        # PolicySpecs and the default RLModuleSpec.
        for module_id in policy_dict | multi_rl_module_spec.module_specs:

            # Remove/skip `learner_only=True` RLModules if `inference_only` is True.
            module_spec = multi_rl_module_spec.module_specs[module_id]
            if inference_only and module_spec.learner_only:
                multi_rl_module_spec.remove_modules(module_id)
                continue

            policy_spec = policy_dict.get(module_id)
            if policy_spec is None:
                policy_spec = policy_dict[DEFAULT_MODULE_ID]

            if module_spec.module_class is None:
                if isinstance(default_rl_module_spec, RLModuleSpec):
                    module_spec.module_class = default_rl_module_spec.module_class
                elif isinstance(default_rl_module_spec.module_specs, RLModuleSpec):
                    module_class = default_rl_module_spec.module_specs.module_class
                    # This should be already checked in validate() but we check it
                    # again here just in case
                    if module_class is None:
                        raise ValueError(
                            "The default rl_module spec cannot have an empty "
                            "module_class under its RLModuleSpec."
                        )
                    module_spec.module_class = module_class
                elif module_id in default_rl_module_spec.module_specs:
                    module_spec.module_class = default_rl_module_spec.module_specs[
                        module_id
                    ].module_class
                else:
                    raise ValueError(
                        f"Module class for module {module_id} cannot be inferred. "
                        f"It is neither provided in the rl_module_spec that "
                        "is passed in nor in the default module spec used in "
                        "the algorithm."
                    )
            if module_spec.catalog_class is None:
                if isinstance(default_rl_module_spec, RLModuleSpec):
                    module_spec.catalog_class = default_rl_module_spec.catalog_class
                elif isinstance(default_rl_module_spec.module_specs, RLModuleSpec):
                    catalog_class = default_rl_module_spec.module_specs.catalog_class
                    module_spec.catalog_class = catalog_class
                elif module_id in default_rl_module_spec.module_specs:
                    module_spec.catalog_class = default_rl_module_spec.module_specs[
                        module_id
                    ].catalog_class
                else:
                    raise ValueError(
                        f"Catalog class for module {module_id} cannot be inferred. "
                        f"It is neither provided in the rl_module_spec that "
                        "is passed in nor in the default module spec used in "
                        "the algorithm."
                    )
            # TODO (sven): Find a good way to pack module specific parameters from
            # the algorithms into the `model_config_dict`.
            if module_spec.observation_space is None:
                module_spec.observation_space = policy_spec.observation_space
            if module_spec.action_space is None:
                module_spec.action_space = policy_spec.action_space
            # In case the `RLModuleSpec` does not have a model config dict, we use the
            # the one defined by the auto keys and the `model_config_dict` arguments in
            # `self.rl_module()`.
            if module_spec.model_config_dict is None:
                module_spec.model_config_dict = self.model_config
            # Otherwise we combine the two dictionaries where settings from the
            # `RLModuleSpec` have higher priority.
            else:
                module_spec.model_config_dict = (
                    self.model_config | module_spec.model_config_dict
                )

        return multi_rl_module_spec

    def __setattr__(self, key, value):
        """Gatekeeper in case we are in frozen state and need to error."""

        # If we are frozen, do not allow to set any attributes anymore.
        if hasattr(self, "_is_frozen") and self._is_frozen:
            # TODO: Remove `simple_optimizer` entirely.
            #  Remove need to set `worker_index` in RolloutWorker's c'tor.
            if key not in ["simple_optimizer", "worker_index", "_is_frozen"]:
                raise AttributeError(
                    f"Cannot set attribute ({key}) of an already frozen "
                    "AlgorithmConfig!"
                )
        # Backward compatibility for checkpoints taken with wheels, in which
        # `self.rl_module_spec` was still settable (now it's a property).
        if key == "rl_module_spec":
            key = "_rl_module_spec"

        super().__setattr__(key, value)

    def __getitem__(self, item):
        """Shim method to still support accessing properties by key lookup.

        This way, an AlgorithmConfig object can still be used as if a dict, e.g.
        by Ray Tune.

        Examples:
            .. testcode::

                from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
                config = AlgorithmConfig()
                print(config["lr"])

            .. testoutput::

                0.001
        """
        # TODO: Uncomment this once all algorithms use AlgorithmConfigs under the
        #  hood (as well as Ray Tune).
        # if log_once("algo_config_getitem"):
        #    logger.warning(
        #        "AlgorithmConfig objects should NOT be used as dict! "
        #        f"Try accessing `{item}` directly as a property."
        #    )
        # In case user accesses "old" keys, e.g. "num_workers", which need to
        # be translated to their correct property names.
        item = self._translate_special_keys(item)
        return getattr(self, item)

    def __setitem__(self, key, value):
        # TODO: Remove comments once all methods/functions only support
        #  AlgorithmConfigs and there is no more ambiguity anywhere in the code
        #  on whether an AlgorithmConfig is used or an old python config dict.
        # raise AttributeError(
        #    "AlgorithmConfig objects should not have their values set like dicts"
        #    f"(`config['{key}'] = {value}`), "
        #    f"but via setting their properties directly (config.{prop} = {value})."
        # )
        if key == "multiagent":
            raise AttributeError(
                "Cannot set `multiagent` key in an AlgorithmConfig!\nTry setting "
                "the multi-agent components of your AlgorithmConfig object via the "
                "`multi_agent()` method and its arguments.\nE.g. `config.multi_agent("
                "policies=.., policy_mapping_fn.., policies_to_train=..)`."
            )
        super().__setattr__(key, value)

    def __contains__(self, item) -> bool:
        """Shim method to help pretend we are a dict."""
        prop = self._translate_special_keys(item, warn_deprecated=False)
        return hasattr(self, prop)

    def get(self, key, default=None):
        """Shim method to help pretend we are a dict."""
        prop = self._translate_special_keys(key, warn_deprecated=False)
        return getattr(self, prop, default)

    def pop(self, key, default=None):
        """Shim method to help pretend we are a dict."""
        return self.get(key, default)

    def keys(self):
        """Shim method to help pretend we are a dict."""
        return self.to_dict().keys()

    def values(self):
        """Shim method to help pretend we are a dict."""
        return self.to_dict().values()

    def items(self):
        """Shim method to help pretend we are a dict."""
        return self.to_dict().items()

    @property
    def model_config(self):
        """Defines the model configuration used.

        This method combines the auto configuration `self _model_config_auto_includes`
        defined by an algorithm with the user-defined configuration in
        `self._model_config_dict`.This configuration dictionary will be used to
        configure the `RLModule` in the new stack and the `ModelV2` in the old
        stack.

        Returns:
            A dictionary with the model configuration.
        """
        return self._model_config_auto_includes | self._model_config_dict

    @property
    def _model_config_auto_includes(self) -> Dict[str, Any]:
        """Defines which `AlgorithmConfig` settings/properties should be
        auto-included into `self.model_config`.

        The dictionary in this property contains the default configuration of an
        algorithm. Together with the `self._model`, this method will be used to
        define the configuration sent to the `RLModule`.

        Returns:
            A dictionary with the automatically included properties/settings of this
            `AlgorithmConfig` object into `self.model_config`.
        """
        return MODEL_DEFAULTS

    # -----------------------------------------------------------
    # Various validation methods for different types of settings.
    # -----------------------------------------------------------
    def _validate_framework_settings(self) -> None:
        """Validates framework settings and checks whether framework is installed."""
        _tf1, _tf, _tfv = None, None, None
        _torch = None
        if self.framework_str not in {"tf", "tf2"} and self.framework_str != "torch":
            return
        elif self.framework_str in {"tf", "tf2"}:
            _tf1, _tf, _tfv = try_import_tf()
        else:
            _torch, _ = try_import_torch()

        # Can not use "tf" with learner API.
        if self.framework_str == "tf" and self.enable_rl_module_and_learner:
            raise ValueError(
                "Cannot use `framework=tf` with the new API stack! Either switch to tf2"
                " via `config.framework('tf2')` OR disable the new API stack via "
                "`config.api_stack(enable_rl_module_and_learner=False)`."
            )

        # Check if torch framework supports torch.compile.
        if (
            _torch is not None
            and self.framework_str == "torch"
            and version.parse(_torch.__version__) < TORCH_COMPILE_REQUIRED_VERSION
            and (self.torch_compile_learner or self.torch_compile_worker)
        ):
            raise ValueError("torch.compile is only supported from torch 2.0.0")

        # Make sure the Learner's torch-what-to-compile setting is supported.
        if self.torch_compile_learner:
            from ray.rllib.core.learner.torch.torch_learner import (
                TorchCompileWhatToCompile,
            )

            if self.torch_compile_learner_what_to_compile not in [
                TorchCompileWhatToCompile.FORWARD_TRAIN,
                TorchCompileWhatToCompile.COMPLETE_UPDATE,
            ]:
                raise ValueError(
                    f"`config.torch_compile_learner_what_to_compile` must be one of ["
                    f"TorchCompileWhatToCompile.forward_train, "
                    f"TorchCompileWhatToCompile.complete_update] but is"
                    f" {self.torch_compile_learner_what_to_compile}"
                )

        self._check_if_correct_nn_framework_installed(_tf1, _tf, _torch)
        self._resolve_tf_settings(_tf1, _tfv)

    def _validate_resources_settings(self):
        """Checks, whether resources related settings make sense."""

        # TODO @Avnishn: This is a short-term work around due to
        #  https://github.com/ray-project/ray/issues/35409
        #  Remove this once we are able to specify placement group bundle index in RLlib
        if self.num_cpus_per_learner > 1 and self.num_gpus_per_learner > 0:
            raise ValueError(
                "Can't set both `num_cpus_per_learner` > 1 and "
                " `num_gpus_per_learner` > 0! Either set "
                "`num_cpus_per_learner` > 1 (and `num_gpus_per_learner`"
                "=0) OR set `num_gpus_per_learner` > 0 (and leave "
                "`num_cpus_per_learner` at its default value of 1). "
                "This is due to issues with placement group fragmentation. See "
                "https://github.com/ray-project/ray/issues/35409 for more details."
            )

        # Make sure the resource requirements for learner_group is valid.
        if self.num_learners == 0 and self.num_gpus_per_env_runner > 1:
            raise ValueError(
                "num_gpus_per_env_runner must be 0 (cpu) or 1 (gpu) when using local "
                "mode (i.e., `num_learners=0`)"
            )

    def _validate_multi_agent_settings(self):
        """Checks, whether multi-agent related settings make sense."""

        # Check `policies_to_train` for invalid entries.
        if isinstance(self.policies_to_train, (list, set, tuple)):
            for pid in self.policies_to_train:
                if pid not in self.policies:
                    raise ValueError(
                        "`config.multi_agent(policies_to_train=..)` contains "
                        f"policy ID ({pid}) that was not defined in "
                        f"`config.multi_agent(policies=..)`!"
                    )

        # TODO (sven): For now, vectorization is not allowed on new EnvRunners with
        #  multi-agent.
        if (
            self.is_multi_agent()
            and self.enable_env_runner_and_connector_v2
            and self.num_envs_per_env_runner > 1
        ):
            raise ValueError(
                "For now, using env vectorization "
                "(`config.num_envs_per_env_runner > 1`) in combination with "
                "multi-agent AND the new EnvRunners is not supported! Try setting "
                "`config.num_envs_per_env_runner = 1`."
            )

    def _validate_evaluation_settings(self):
        """Checks, whether evaluation related settings make sense."""

        # Async evaluation has been deprecated. Use "simple" parallel mode instead
        # (which is also async):
        # `config.evaluation(evaluation_parallel_to_training=True)`.
        if self.enable_async_evaluation is True:
            raise ValueError(
                "`enable_async_evaluation` has been deprecated (you should set this to "
                "False)! Use `config.evaluation(evaluation_parallel_to_training=True)` "
                "instead."
            )

        # If `evaluation_num_env_runners` > 0, warn if `evaluation_interval` is 0 or
        # None.
        if self.evaluation_num_env_runners > 0 and not self.evaluation_interval:
            logger.warning(
                f"You have specified {self.evaluation_num_env_runners} "
                "evaluation workers, but your `evaluation_interval` is 0 or None! "
                "Therefore, evaluation will not occur automatically with each"
                " call to `Algorithm.train()`. Instead, you will have to call "
                "`Algorithm.evaluate()` manually in order to trigger an "
                "evaluation run."
            )
        # If `evaluation_num_env_runners=0` and
        # `evaluation_parallel_to_training=True`, warn that you need
        # at least one remote eval worker for parallel training and
        # evaluation, and set `evaluation_parallel_to_training` to False.
        if (
            self.evaluation_num_env_runners == 0
            and self.evaluation_parallel_to_training
        ):
            raise ValueError(
                "`evaluation_parallel_to_training` can only be done if "
                "`evaluation_num_env_runners` > 0! Try setting "
                "`config.evaluation_parallel_to_training` to False."
            )

        # If `evaluation_duration=auto`, error if
        # `evaluation_parallel_to_training=False`.
        if self.evaluation_duration == "auto":
            if not self.evaluation_parallel_to_training:
                raise ValueError(
                    "`evaluation_duration=auto` not supported for "
                    "`evaluation_parallel_to_training=False`!"
                )
            elif self.evaluation_duration_unit == "episodes":
                logger.warning(
                    "When using `config.evaluation_duration='auto'`, the sampling unit "
                    "used is always 'timesteps'! You have set "
                    "`config.evaluation_duration_unit='episodes'`, which will be "
                    "ignored."
                )

        # Make sure, `evaluation_duration` is an int otherwise.
        elif (
            not isinstance(self.evaluation_duration, int)
            or self.evaluation_duration <= 0
        ):
            raise ValueError(
                f"`evaluation_duration` ({self.evaluation_duration}) must be an "
                f"int and >0!"
            )

    def _validate_input_settings(self):
        """Checks, whether input related settings make sense."""

        if self.input_ == "sampler" and self.off_policy_estimation_methods:
            raise ValueError(
                "Off-policy estimation methods can only be used if the input is a "
                "dataset. We currently do not support applying off_policy_estimation_"
                "method on a sampler input."
            )

        if self.input_ == "dataset":
            # If you need to read a Ray dataset set the parallelism and
            # num_cpus_per_read_task from rollout worker settings
            self.input_config["num_cpus_per_read_task"] = self.num_cpus_per_env_runner
            if self.in_evaluation:
                # If using dataset for evaluation, the parallelism gets set to
                # evaluation_num_env_runners for backward compatibility and num_cpus
                # gets set to num_cpus_per_env_runner from rollout worker. User only
                # needs to set evaluation_num_env_runners.
                self.input_config["parallelism"] = self.evaluation_num_env_runners or 1
            else:
                # If using dataset for training, the parallelism and num_cpus gets set
                # based on rollout worker parameters. This is for backwards
                # compatibility for now. User only needs to set num_env_runners.
                self.input_config["parallelism"] = self.num_env_runners or 1

    def _validate_new_api_stack_settings(self):
        """Checks, whether settings related to the new API stack make sense."""

        if not self.enable_rl_module_and_learner:
            # Throw a warning if the user has used `self.rl_module(rl_module_spec=...)`
            # but has not enabled the new API stack at the same time.
            if self._rl_module_spec is not None:
                logger.warning(
                    "You have setup a RLModuleSpec (via calling "
                    "`config.rl_module(...)`), but have not enabled the new API stack. "
                    "To enable it, call `config.api_stack(enable_rl_module_and_learner="
                    "True)`."
                )
            # Throw a warning if the user has used `self.training(learner_class=...)`
            # but has not enabled the new API stack at the same time.
            if self._learner_class is not None:
                logger.warning(
                    "You specified a custom Learner class (via "
                    f"`AlgorithmConfig.training(learner_class={self._learner_class})`, "
                    f"but have the new API stack disabled. You need to enable it via "
                    "`AlgorithmConfig.api_stack(enable_rl_module_and_learner=True)`."
                )
            # User is using the new EnvRunners, but forgot to switch on
            # `enable_rl_module_and_learner`.
            if self.enable_env_runner_and_connector_v2:
                raise ValueError(
                    "You are using the new API stack EnvRunners (SingleAgentEnvRunner "
                    "or MultiAgentEnvRunner), but have forgotten to switch on the new "
                    "API stack! Try setting "
                    "`config.api_stack(enable_rl_module_and_learner=True)`."
                )
            # Early out. The rest of this method is only for
            # `enable_rl_module_and_learner=True`.
            return

        # New API stack (RLModule, Learner APIs) only works with connectors.
        if not self.enable_connectors:
            raise ValueError(
                "The new API stack (RLModule and Learner APIs) only works with "
                "connectors! Please enable connectors via "
                "`config.env_runners(enable_connectors=True)`."
            )

        # LR-schedule checking.
        Scheduler.validate(
            fixed_value_or_schedule=self.lr,
            setting_name="lr",
            description="learning rate",
        )

        # Check and error if `on_episode_created` callback has been overridden on the
        # new API stack AND this is a single-agent setup (multi-agent does not use
        # gym.vector.Env yet and therefore the reset call is still made manually,
        # allowing for the callback to be fired).
        if (
            self.enable_env_runner_and_connector_v2
            and not self.is_multi_agent()
            and self.callbacks_class is not DefaultCallbacks
        ):
            default_src = inspect.getsource(DefaultCallbacks.on_episode_created)
            try:
                user_src = inspect.getsource(self.callbacks_class.on_episode_created)
            # In case user has setup a `partial` instead of an actual Callbacks class.
            except AttributeError:
                user_src = default_src
            if default_src != user_src:
                raise ValueError(
                    "When using the new API stack in single-agent and with EnvRunners, "
                    "you cannot override the `DefaultCallbacks.on_episode_created()` "
                    "method anymore! This particular callback is no longer supported "
                    "b/c we are using `gym.vector.Env`, which automatically resets "
                    "individual sub-environments when they are terminated. Instead, "
                    "override the `on_episode_start` method, which gets fired right "
                    "after the `env.reset()` call."
                )

        # This is not compatible with RLModules, which all have a method
        # `forward_exploration` to specify custom exploration behavior.
        if self.exploration_config:
            raise ValueError(
                "When the RLModule API is enabled, exploration_config can not be "
                "set. If you want to implement custom exploration behaviour, "
                "please modify the `forward_exploration` method of the "
                "RLModule at hand. On configs that have a default exploration "
                "config, this must be done via "
                "`config.exploration_config={}`."
            )

        not_compatible_w_rlm_msg = (
            "Cannot use `{}` option with the new API stack (RLModule and "
            "Learner APIs)! `{}` is part of the ModelV2 API and Policy API,"
            " which are not compatible with the new API stack. You can either "
            "deactivate the new stack via `config.api_stack( "
            "enable_rl_module_and_learner=False)`,"
            "or use the new stack (incl. RLModule API) and implement your "
            "custom model as an RLModule."
        )

        if self.model["custom_model"] is not None:
            raise ValueError(
                not_compatible_w_rlm_msg.format("custom_model", "custom_model")
            )

        if self.model["custom_model_config"] != {}:
            raise ValueError(
                not_compatible_w_rlm_msg.format(
                    "custom_model_config", "custom_model_config"
                )
            )

    # TODO (sven): Once everything is on the new API stack, we won't need this method
    #  anymore.
    def _validate_to_be_deprecated_settings(self):
        # Env task fn will be deprecated.
        if self.enable_rl_module_and_learner and self.env_task_fn is not None:
            deprecation_warning(
                old="AlgorithmConfig.env_task_fn",
                help="The `env_task_fn` API is not supported on the new API stack! "
                "Curriculum learning should instead be implemented solely via "
                "custom callbacks. Check out our curriculum learning example "
                "script for more information: "
                "https://github.com/ray-project/ray/blob/master/rllib/examples/curriculum/curriculum_learning.py",  # noqa
            )
        # `render_env` is deprecated on new API stack.
        if self.enable_env_runner_and_connector_v2 and self.render_env is not False:
            deprecation_warning(
                old="AlgorithmConfig.render_env",
                help="The `render_env` setting is not supported on the new API stack! "
                "In order to log videos to WandB (or other loggers), take a look at "
                "this example here: "
                "https://github.com/ray-project/ray/blob/master/rllib/examples/envs/env_rendering_and_recording.py",  # noqa
            )

        if self.preprocessor_pref not in ["rllib", "deepmind", None]:
            raise ValueError(
                "`config.preprocessor_pref` must be either 'rllib', 'deepmind' or None!"
            )

        # Check model config.
        # If no preprocessing, propagate into model's config as well
        # (so model will know, whether inputs are preprocessed or not).
        if self._disable_preprocessor_api is True:
            self.model["_disable_preprocessor_api"] = True
        # If no action flattening, propagate into model's config as well
        # (so model will know, whether action inputs are already flattened or
        # not).
        if self._disable_action_flattening is True:
            self.model["_disable_action_flattening"] = True
        if self.model.get("custom_preprocessor"):
            deprecation_warning(
                old="AlgorithmConfig.training(model={'custom_preprocessor': ...})",
                help="Custom preprocessors are deprecated, "
                "since they sometimes conflict with the built-in "
                "preprocessors for handling complex observation spaces. "
                "Please use wrapper classes around your environment "
                "instead.",
                error=True,
            )

        # Multi-GPU settings.
        if self.simple_optimizer is True:
            pass
        # Multi-GPU setting: Must use MultiGPUTrainOneStep.
        elif not self.enable_rl_module_and_learner and self.num_gpus > 1:
            # TODO: AlphaStar uses >1 GPUs differently (1 per policy actor), so this is
            #  ok for tf2 here.
            #  Remove this hacky check, once we have fully moved to the Learner API.
            if self.framework_str == "tf2" and type(self).__name__ != "AlphaStar":
                raise ValueError(
                    "`num_gpus` > 1 not supported yet for "
                    f"framework={self.framework_str}!"
                )
            elif self.simple_optimizer is True:
                raise ValueError(
                    "Cannot use `simple_optimizer` if `num_gpus` > 1! "
                    "Consider not setting `simple_optimizer` in your config."
                )
            self.simple_optimizer = False
        # Auto-setting: Use simple-optimizer for tf-eager or multiagent,
        # otherwise: MultiGPUTrainOneStep (if supported by the algo's execution
        # plan).
        elif self.simple_optimizer == DEPRECATED_VALUE:
            # tf-eager: Must use simple optimizer.
            if self.framework_str not in ["tf", "torch"]:
                self.simple_optimizer = True
            # Multi-agent case: Try using MultiGPU optimizer (only
            # if all policies used are DynamicTFPolicies or TorchPolicies).
            elif self.is_multi_agent():
                from ray.rllib.policy.dynamic_tf_policy import DynamicTFPolicy
                from ray.rllib.policy.torch_policy import TorchPolicy

                default_policy_cls = None
                if self.algo_class:
                    default_policy_cls = self.algo_class.get_default_policy_class(self)

                policies = self.policies
                policy_specs = (
                    [
                        PolicySpec(*spec) if isinstance(spec, (tuple, list)) else spec
                        for spec in policies.values()
                    ]
                    if isinstance(policies, dict)
                    else [PolicySpec() for _ in policies]
                )

                if any(
                    (spec.policy_class or default_policy_cls) is None
                    or not issubclass(
                        spec.policy_class or default_policy_cls,
                        (DynamicTFPolicy, TorchPolicy),
                    )
                    for spec in policy_specs
                ):
                    self.simple_optimizer = True
                else:
                    self.simple_optimizer = False
            else:
                self.simple_optimizer = False

        # User manually set simple-optimizer to False -> Error if tf-eager.
        elif self.simple_optimizer is False:
            if self.framework_str == "tf2":
                raise ValueError(
                    "`simple_optimizer=False` not supported for "
                    f"config.framework({self.framework_str})!"
                )

    def _validate_offline_settings(self):
        from ray.rllib.offline.offline_prelearner import OfflinePreLearner

        if self.prelearner_class and not issubclass(
            self.prelearner_class, OfflinePreLearner
        ):
            raise ValueError(
                "Unknown `prelearner_class`. Prelearner class needs to inherit "
                "from `OfflinePreLearner` class."
            )

        from ray.rllib.utils.replay_buffers.episode_replay_buffer import (
            EpisodeReplayBuffer,
        )

        if self.prelearner_buffer_class and not issubclass(
            self.prelearner_buffer_class, EpisodeReplayBuffer
        ):
            raise ValueError(
                "Unknown `prelearner_buffer_class`. The buffer class for the "
                "prelearner needs to inherit from `EpisodeReplayBuffer`. "
                "Specifically it needs to store and sample lists of "
                "`Single-/MultiAgentEpisode`s."
            )

    @staticmethod
    def _serialize_dict(config):
        # Serialize classes to classpaths:
        if "callbacks_class" in config:
            config["callbacks"] = config.pop("callbacks_class")
        if "class" in config:
            config["class"] = serialize_type(config["class"])
        config["callbacks"] = serialize_type(config["callbacks"])
        config["sample_collector"] = serialize_type(config["sample_collector"])
        if isinstance(config["env"], type):
            config["env"] = serialize_type(config["env"])
        if "replay_buffer_config" in config and (
            isinstance(config["replay_buffer_config"].get("type"), type)
        ):
            config["replay_buffer_config"]["type"] = serialize_type(
                config["replay_buffer_config"]["type"]
            )
        if isinstance(config["exploration_config"].get("type"), type):
            config["exploration_config"]["type"] = serialize_type(
                config["exploration_config"]["type"]
            )
        if isinstance(config["model"].get("custom_model"), type):
            config["model"]["custom_model"] = serialize_type(
                config["model"]["custom_model"]
            )

        # List'ify `policies`, iff a set or tuple (these types are not JSON'able).
        ma_config = config.get("multiagent")
        if ma_config is not None:
            if isinstance(ma_config.get("policies"), (set, tuple)):
                ma_config["policies"] = list(ma_config["policies"])
            # Do NOT serialize functions/lambdas.
            if ma_config.get("policy_mapping_fn"):
                ma_config["policy_mapping_fn"] = NOT_SERIALIZABLE
            if ma_config.get("policies_to_train"):
                ma_config["policies_to_train"] = NOT_SERIALIZABLE
        # However, if these "multiagent" settings have been provided directly
        # on the top-level (as they should), we override the settings under
        # "multiagent". Note that the "multiagent" key should no longer be used anyways.
        if isinstance(config.get("policies"), (set, tuple)):
            config["policies"] = list(config["policies"])
        # Do NOT serialize functions/lambdas.
        if config.get("policy_mapping_fn"):
            config["policy_mapping_fn"] = NOT_SERIALIZABLE
        if config.get("policies_to_train"):
            config["policies_to_train"] = NOT_SERIALIZABLE

        return config

    @staticmethod
    def _translate_special_keys(key: str, warn_deprecated: bool = True) -> str:
        # Handle special key (str) -> `AlgorithmConfig.[some_property]` cases.
        if key == "callbacks":
            key = "callbacks_class"
        elif key == "create_env_on_driver":
            key = "create_env_on_local_worker"
        elif key == "custom_eval_function":
            key = "custom_evaluation_function"
        elif key == "framework":
            key = "framework_str"
        elif key == "input":
            key = "input_"
        elif key == "lambda":
            key = "lambda_"
        elif key == "num_cpus_for_driver":
            key = "num_cpus_for_main_process"
        elif key == "num_workers":
            key = "num_env_runners"

        # Deprecated keys.
        if warn_deprecated:
            if key == "collect_metrics_timeout":
                deprecation_warning(
                    old="collect_metrics_timeout",
                    new="metrics_episode_collection_timeout_s",
                    error=True,
                )
            elif key == "metrics_smoothing_episodes":
                deprecation_warning(
                    old="config.metrics_smoothing_episodes",
                    new="config.metrics_num_episodes_for_smoothing",
                    error=True,
                )
            elif key == "min_iter_time_s":
                deprecation_warning(
                    old="config.min_iter_time_s",
                    new="config.min_time_s_per_iteration",
                    error=True,
                )
            elif key == "min_time_s_per_reporting":
                deprecation_warning(
                    old="config.min_time_s_per_reporting",
                    new="config.min_time_s_per_iteration",
                    error=True,
                )
            elif key == "min_sample_timesteps_per_reporting":
                deprecation_warning(
                    old="config.min_sample_timesteps_per_reporting",
                    new="config.min_sample_timesteps_per_iteration",
                    error=True,
                )
            elif key == "min_train_timesteps_per_reporting":
                deprecation_warning(
                    old="config.min_train_timesteps_per_reporting",
                    new="config.min_train_timesteps_per_iteration",
                    error=True,
                )
            elif key == "timesteps_per_iteration":
                deprecation_warning(
                    old="config.timesteps_per_iteration",
                    new="`config.min_sample_timesteps_per_iteration` OR "
                    "`config.min_train_timesteps_per_iteration`",
                    error=True,
                )
            elif key == "evaluation_num_episodes":
                deprecation_warning(
                    old="config.evaluation_num_episodes",
                    new="`config.evaluation_duration` and "
                    "`config.evaluation_duration_unit=episodes`",
                    error=True,
                )

        return key

    def _check_if_correct_nn_framework_installed(self, _tf1, _tf, _torch):
        """Check if tf/torch experiment is running and tf/torch installed."""
        if self.framework_str in {"tf", "tf2"}:
            if not (_tf1 or _tf):
                raise ImportError(
                    (
                        "TensorFlow was specified as the framework to use (via `config."
                        "framework([tf|tf2])`)! However, no installation was "
                        "found. You can install TensorFlow via `pip install tensorflow`"
                    )
                )
        elif self.framework_str == "torch":
            if not _torch:
                raise ImportError(
                    (
                        "PyTorch was specified as the framework to use (via `config."
                        "framework('torch')`)! However, no installation was found. You "
                        "can install PyTorch via `pip install torch`."
                    )
                )

    def _resolve_tf_settings(self, _tf1, _tfv):
        """Check and resolve tf settings."""
        if _tf1 and self.framework_str == "tf2":
            if self.framework_str == "tf2" and _tfv < 2:
                raise ValueError(
                    "You configured `framework`=tf2, but your installed "
                    "pip tf-version is < 2.0! Make sure your TensorFlow "
                    "version is >= 2.x."
                )
            if not _tf1.executing_eagerly():
                _tf1.enable_eager_execution()
            # Recommend setting tracing to True for speedups.
            logger.info(
                f"Executing eagerly (framework='{self.framework_str}'),"
                f" with eager_tracing={self.eager_tracing}. For "
                "production workloads, make sure to set eager_tracing=True"
                "  in order to match the speed of tf-static-graph "
                "(framework='tf'). For debugging purposes, "
                "`eager_tracing=False` is the best choice."
            )
        # Tf-static-graph (framework=tf): Recommend upgrading to tf2 and
        # enabling eager tracing for similar speed.
        elif _tf1 and self.framework_str == "tf":
            logger.info(
                "Your framework setting is 'tf', meaning you are using "
                "static-graph mode. Set framework='tf2' to enable eager "
                "execution with tf2.x. You may also then want to set "
                "eager_tracing=True in order to reach similar execution "
                "speed as with static-graph mode."
            )

    @OldAPIStack
    def get_multi_agent_setup(
        self,
        *,
        policies: Optional[MultiAgentPolicyConfigDict] = None,
        env: Optional[EnvType] = None,
        spaces: Optional[Dict[PolicyID, Tuple[Space, Space]]] = None,
        default_policy_class: Optional[Type[Policy]] = None,
    ) -> Tuple[MultiAgentPolicyConfigDict, Callable[[PolicyID, SampleBatchType], bool]]:
        r"""Compiles complete multi-agent config (dict) from the information in `self`.

        Infers the observation- and action spaces, the policy classes, and the policy's
        configs. The returned `MultiAgentPolicyConfigDict` is fully unified and strictly
        maps PolicyIDs to complete PolicySpec objects (with all their fields not-None).

        Examples:
        .. testcode::

            import gymnasium as gym
            from ray.rllib.algorithms.ppo import PPOConfig
            config = (
              PPOConfig()
              .environment("CartPole-v1")
              .framework("torch")
              .multi_agent(policies={"pol1", "pol2"}, policies_to_train=["pol1"])
            )
            policy_dict, is_policy_to_train = config.get_multi_agent_setup(
                env=gym.make("CartPole-v1"))
            is_policy_to_train("pol1")
            is_policy_to_train("pol2")

        Args:
            policies: An optional multi-agent `policies` dict, mapping policy IDs
                to PolicySpec objects. If not provided, will use `self.policies`
                instead. Note that the `policy_class`, `observation_space`, and
                `action_space` properties in these PolicySpecs may be None and must
                therefore be inferred here.
            env: An optional env instance, from which to infer the different spaces for
                the different policies. If not provided, will try to infer from
                `spaces`. Otherwise from `self.observation_space` and
                `self.action_space`. If no information on spaces can be infered, will
                raise an error.
            spaces: Optional dict mapping policy IDs to tuples of 1) observation space
                and 2) action space that should be used for the respective policy.
                These spaces were usually provided by an already instantiated remote
                EnvRunner. Note that if the `env` argument is provided, will try to
                infer spaces from `env` first.
            default_policy_class: The Policy class to use should a PolicySpec have its
                policy_class property set to None.

        Returns:
            A tuple consisting of 1) a MultiAgentPolicyConfigDict and 2) a
            `is_policy_to_train(PolicyID, SampleBatchType) -> bool` callable.

        Raises:
            ValueError: In case, no spaces can be infered for the policy/ies.
            ValueError: In case, two agents in the env map to the same PolicyID
                (according to `self.policy_mapping_fn`), but have different action- or
                observation spaces according to the infered space information.
        """
        policies = copy.deepcopy(policies or self.policies)

        # Policies given as set/list/tuple (of PolicyIDs) -> Setup each policy
        # automatically via empty PolicySpec (will make RLlib infer observation- and
        # action spaces as well as the Policy's class).
        if isinstance(policies, (set, list, tuple)):
            policies = {pid: PolicySpec() for pid in policies}

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
            # `env` is a gymnasium.vector.Env.
            if hasattr(env, "single_observation_space") and isinstance(
                env.single_observation_space, gym.Space
            ):
                env_obs_space = env.single_observation_space
            # `env` is a gymnasium.Env.
            elif hasattr(env, "observation_space") and isinstance(
                env.observation_space, gym.Space
            ):
                env_obs_space = env.observation_space

            # `env` is a gymnasium.vector.Env.
            if hasattr(env, "single_action_space") and isinstance(
                env.single_action_space, gym.Space
            ):
                env_act_space = env.single_action_space
            # `env` is a gymnasium.Env.
            elif hasattr(env, "action_space") and isinstance(
                env.action_space, gym.Space
            ):
                env_act_space = env.action_space

        # Last resort: Try getting the env's spaces from the spaces
        # dict's special __env__ key.
        if spaces is not None:
            if env_obs_space is None:
                env_obs_space = spaces.get(INPUT_ENV_SPACES, [None])[0]
            if env_act_space is None:
                env_act_space = spaces.get(INPUT_ENV_SPACES, [None, None])[1]

        # Check each defined policy ID and unify its spec.
        for pid, policy_spec in policies.copy().items():
            # Convert to PolicySpec if plain list/tuple.
            if not isinstance(policy_spec, PolicySpec):
                policies[pid] = policy_spec = PolicySpec(*policy_spec)

            # Infer policy classes for policies dict, if not provided (None).
            if policy_spec.policy_class is None and default_policy_class is not None:
                policies[pid].policy_class = default_policy_class

            # Infer observation space.
            if policy_spec.observation_space is None:
                if spaces is not None and pid in spaces:
                    obs_space = spaces[pid][0]
                elif env_obs_space is not None:
                    env_unwrapped = env.unwrapped if hasattr(env, "unwrapped") else env
                    # Multi-agent case AND different agents have different spaces:
                    # Need to reverse map spaces (for the different agents) to certain
                    # policy IDs.
                    if (
                        isinstance(env_unwrapped, MultiAgentEnv)
                        and hasattr(env_unwrapped, "_obs_space_in_preferred_format")
                        and env_unwrapped._obs_space_in_preferred_format
                    ):
                        obs_space = None
                        mapping_fn = self.policy_mapping_fn
                        one_obs_space = next(iter(env_obs_space.values()))
                        # If all obs spaces are the same anyways, just use the first
                        # single-agent space.
                        if all(s == one_obs_space for s in env_obs_space.values()):
                            obs_space = one_obs_space
                        # Otherwise, we have to compare the ModuleID with all possible
                        # AgentIDs and find the agent ID that matches.
                        elif mapping_fn:
                            for aid in env_unwrapped.get_agent_ids():
                                # Match: Assign spaces for this agentID to the PolicyID.
                                if mapping_fn(aid, None, worker=None) == pid:
                                    # Make sure, different agents that map to the same
                                    # policy don't have different spaces.
                                    if (
                                        obs_space is not None
                                        and env_obs_space[aid] != obs_space
                                    ):
                                        raise ValueError(
                                            "Two agents in your environment map to the "
                                            "same policyID (as per your `policy_mapping"
                                            "_fn`), however, these agents also have "
                                            "different observation spaces!"
                                        )
                                    obs_space = env_obs_space[aid]
                    # Otherwise, just use env's obs space as-is.
                    else:
                        obs_space = env_obs_space
                # Space given directly in config.
                elif self.observation_space:
                    obs_space = self.observation_space
                else:
                    raise ValueError(
                        "`observation_space` not provided in PolicySpec for "
                        f"{pid} and env does not have an observation space OR "
                        "no spaces received from other workers' env(s) OR no "
                        "`observation_space` specified in config!"
                    )

                policies[pid].observation_space = obs_space

            # Infer action space.
            if policy_spec.action_space is None:
                if spaces is not None and pid in spaces:
                    act_space = spaces[pid][1]
                elif env_act_space is not None:
                    env_unwrapped = env.unwrapped if hasattr(env, "unwrapped") else env
                    # Multi-agent case AND different agents have different spaces:
                    # Need to reverse map spaces (for the different agents) to certain
                    # policy IDs.
                    if (
                        isinstance(env_unwrapped, MultiAgentEnv)
                        and hasattr(env_unwrapped, "_action_space_in_preferred_format")
                        and env_unwrapped._action_space_in_preferred_format
                    ):
                        act_space = None
                        mapping_fn = self.policy_mapping_fn
                        one_act_space = next(iter(env_act_space.values()))
                        # If all action spaces are the same anyways, just use the first
                        # single-agent space.
                        if all(s == one_act_space for s in env_act_space.values()):
                            act_space = one_act_space
                        # Otherwise, we have to compare the ModuleID with all possible
                        # AgentIDs and find the agent ID that matches.
                        elif mapping_fn:
                            for aid in env_unwrapped.get_agent_ids():
                                # Match: Assign spaces for this AgentID to the PolicyID.
                                if mapping_fn(aid, None, worker=None) == pid:
                                    # Make sure, different agents that map to the same
                                    # policy don't have different spaces.
                                    if (
                                        act_space is not None
                                        and env_act_space[aid] != act_space
                                    ):
                                        raise ValueError(
                                            "Two agents in your environment map to the "
                                            "same policyID (as per your `policy_mapping"
                                            "_fn`), however, these agents also have "
                                            "different action spaces!"
                                        )
                                    act_space = env_act_space[aid]
                    # Otherwise, just use env's action space as-is.
                    else:
                        act_space = env_act_space
                elif self.action_space:
                    act_space = self.action_space
                else:
                    raise ValueError(
                        "`action_space` not provided in PolicySpec for "
                        f"{pid} and env does not have an action space OR "
                        "no spaces received from other workers' env(s) OR no "
                        "`action_space` specified in config!"
                    )
                policies[pid].action_space = act_space

            # Create entire AlgorithmConfig object from the provided override.
            # If None, use {} as override.
            if not isinstance(policies[pid].config, AlgorithmConfig):
                assert policies[pid].config is None or isinstance(
                    policies[pid].config, dict
                )
                policies[pid].config = self.copy(copy_frozen=False).update_from_dict(
                    policies[pid].config or {}
                )

        # If collection given, construct a simple default callable returning True
        # if the PolicyID is found in the list/set of IDs.
        if self.policies_to_train is not None and not callable(self.policies_to_train):
            pols = set(self.policies_to_train)

            def is_policy_to_train(pid, batch=None):
                return pid in pols

        else:
            is_policy_to_train = self.policies_to_train

        return policies, is_policy_to_train

    @Deprecated(new="AlgorithmConfig.get_multi_rl_module_spec()", error=False)
    def get_marl_module_spec(self, *args, **kwargs):
        return self.get_multi_rl_module_spec(*args, **kwargs)

    @Deprecated(new="AlgorithmConfig.env_runners(..)", error=False)
    def rollouts(self, *args, **kwargs):
        return self.env_runners(*args, **kwargs)

    @Deprecated(new="AlgorithmConfig.env_runners(..)", error=False)
    def exploration(self, *args, **kwargs):
        return self.env_runners(*args, **kwargs)

    @property
    @Deprecated(new="AlgorithmConfig._enable_new_api_stack", error=False)
    def _enable_new_api_stack(self):
        return self.enable_rl_module_and_learner

    @_enable_new_api_stack.setter
    def _enable_new_api_stack(self, value):
        deprecation_warning(
            old="AlgorithmConfig._enable_new_api_stack",
            new="AlgorithmConfig.enable_rl_module_and_learner",
            error=False,
        )
        self.enable_rl_module_and_learner = value

    @property
    @Deprecated(new="AlgorithmConfig.enable_env_runner_and_connector_v2", error=True)
    def uses_new_env_runners(self):
        pass

    @property
    @Deprecated(new="AlgorithmConfig.num_env_runners", error=False)
    def num_rollout_workers(self):
        return self.num_env_runners

    @num_rollout_workers.setter
    def num_rollout_workers(self, value):
        deprecation_warning(
            old="AlgorithmConfig.num_rollout_workers",
            new="AlgorithmConfig.num_env_runners",
            error=False,
        )
        self.num_env_runners = value

    @property
    @Deprecated(new="AlgorithmConfig.evaluation_num_workers", error=False)
    def evaluation_num_workers(self):
        return self.evaluation_num_env_runners

    @evaluation_num_workers.setter
    def evaluation_num_workers(self, value):
        deprecation_warning(
            old="AlgorithmConfig.evaluation_num_workers",
            new="AlgorithmConfig.evaluation_num_env_runners",
            error=False,
        )
        self.evaluation_num_env_runners = value

    @property
    @Deprecated(new="AlgorithmConfig.num_envs_per_env_runner", error=False)
    def num_envs_per_worker(self):
        return self.num_envs_per_env_runner

    @num_envs_per_worker.setter
    def num_envs_per_worker(self, value):
        deprecation_warning(
            old="AlgorithmConfig.num_envs_per_worker",
            new="AlgorithmConfig.num_envs_per_env_runner",
            error=False,
        )
        self.num_envs_per_env_runner = value

    @property
    @Deprecated(new="AlgorithmConfig.ignore_env_runner_failures", error=False)
    def ignore_worker_failures(self):
        return self.ignore_env_runner_failures

    @ignore_worker_failures.setter
    def ignore_worker_failures(self, value):
        deprecation_warning(
            old="AlgorithmConfig.ignore_worker_failures",
            new="AlgorithmConfig.ignore_env_runner_failures",
            error=False,
        )
        self.ignore_env_runner_failures = value

    @property
    @Deprecated(new="AlgorithmConfig.recreate_failed_env_runners", error=False)
    def recreate_failed_workers(self):
        return self.recreate_failed_env_runners

    @recreate_failed_workers.setter
    def recreate_failed_workers(self, value):
        deprecation_warning(
            old="AlgorithmConfig.recreate_failed_workers",
            new="AlgorithmConfig.recreate_failed_env_runners",
            error=False,
        )
        self.recreate_failed_env_runners = value

    @property
    @Deprecated(new="AlgorithmConfig.max_num_env_runner_restarts", error=False)
    def max_num_worker_restarts(self):
        return self.max_num_env_runner_restarts

    @max_num_worker_restarts.setter
    def max_num_worker_restarts(self, value):
        deprecation_warning(
            old="AlgorithmConfig.max_num_worker_restarts",
            new="AlgorithmConfig.max_num_env_runner_restarts",
            error=False,
        )
        self.max_num_env_runner_restarts = value

    @property
    @Deprecated(new="AlgorithmConfig.delay_between_env_runner_restarts_s", error=False)
    def delay_between_worker_restarts_s(self):
        return self.delay_between_env_runner_restarts_s

    @delay_between_worker_restarts_s.setter
    def delay_between_worker_restarts_s(self, value):
        deprecation_warning(
            old="AlgorithmConfig.delay_between_worker_restarts_s",
            new="AlgorithmConfig.delay_between_env_runner_restarts_s",
            error=False,
        )
        self.delay_between_env_runner_restarts_s = value

    @property
    @Deprecated(
        new="AlgorithmConfig.num_consecutive_env_runner_failures_tolerance", error=False
    )
    def num_consecutive_worker_failures_tolerance(self):
        return self.num_consecutive_env_runner_failures_tolerance

    @num_consecutive_worker_failures_tolerance.setter
    def num_consecutive_worker_failures_tolerance(self, value):
        deprecation_warning(
            old="AlgorithmConfig.num_consecutive_worker_failures_tolerance",
            new="AlgorithmConfig.num_consecutive_env_runner_failures_tolerance",
            error=False,
        )
        self.num_consecutive_env_runner_failures_tolerance = value

    @property
    @Deprecated(new="AlgorithmConfig.env_runner_health_probe_timeout_s", error=False)
    def worker_health_probe_timeout_s(self):
        return self.env_runner_health_probe_timeout_s

    @worker_health_probe_timeout_s.setter
    def worker_health_probe_timeout_s(self, value):
        deprecation_warning(
            old="AlgorithmConfig.worker_health_probe_timeout_s",
            new="AlgorithmConfig.env_runner_health_probe_timeout_s",
            error=False,
        )
        self.env_runner_health_probe_timeout_s = value

    @property
    @Deprecated(new="AlgorithmConfig.env_runner_restore_timeout_s", error=False)
    def worker_restore_timeout_s(self):
        return self.env_runner_restore_timeout_s

    @worker_restore_timeout_s.setter
    def worker_restore_timeout_s(self, value):
        deprecation_warning(
            old="AlgorithmConfig.worker_restore_timeout_s",
            new="AlgorithmConfig.env_runner_restore_timeout_s",
            error=False,
        )
        self.env_runner_restore_timeout_s = value

    @property
    @Deprecated(
        new="AlgorithmConfig.validate_env_runners_after_construction",
        error=False,
    )
    def validate_workers_after_construction(self):
        return self.validate_env_runners_after_construction

    @validate_workers_after_construction.setter
    def validate_workers_after_construction(self, value):
        deprecation_warning(
            old="AlgorithmConfig.validate_workers_after_construction",
            new="AlgorithmConfig.validate_env_runners_after_construction",
            error=False,
        )
        self.validate_env_runners_after_construction = value

    # Cleanups from `resources()`.
    @property
    @Deprecated(new="AlgorithmConfig.num_cpus_per_env_runner", error=False)
    def num_cpus_per_worker(self):
        return self.num_cpus_per_env_runner

    @num_cpus_per_worker.setter
    def num_cpus_per_worker(self, value):
        deprecation_warning(
            old="AlgorithmConfig.num_cpus_per_worker",
            new="AlgorithmConfig.num_cpus_per_env_runner",
            error=False,
        )
        self.num_cpus_per_env_runner = value

    @property
    @Deprecated(new="AlgorithmConfig.num_gpus_per_env_runner", error=False)
    def num_gpus_per_worker(self):
        return self.num_gpus_per_env_runner

    @num_gpus_per_worker.setter
    def num_gpus_per_worker(self, value):
        deprecation_warning(
            old="AlgorithmConfig.num_gpus_per_worker",
            new="AlgorithmConfig.num_gpus_per_env_runner",
            error=False,
        )
        self.num_gpus_per_env_runner = value

    @property
    @Deprecated(new="AlgorithmConfig.custom_resources_per_env_runner", error=False)
    def custom_resources_per_worker(self):
        return self.custom_resources_per_env_runner

    @custom_resources_per_worker.setter
    def custom_resources_per_worker(self, value):
        deprecation_warning(
            old="AlgorithmConfig.custom_resources_per_worker",
            new="AlgorithmConfig.custom_resources_per_env_runner",
            error=False,
        )
        self.custom_resources_per_env_runner = value

    @property
    @Deprecated(new="AlgorithmConfig.num_learners", error=False)
    def num_learner_workers(self):
        return self.num_learners

    @num_learner_workers.setter
    def num_learner_workers(self, value):
        deprecation_warning(
            old="AlgorithmConfig.num_learner_workers",
            new="AlgorithmConfig.num_learners",
            error=False,
        )
        self.num_learners = value

    @property
    @Deprecated(new="AlgorithmConfig.num_cpus_per_learner", error=False)
    def num_cpus_per_learner_worker(self):
        return self.num_cpus_per_learner

    @num_cpus_per_learner_worker.setter
    def num_cpus_per_learner_worker(self, value):
        deprecation_warning(
            old="AlgorithmConfig.num_cpus_per_learner_worker",
            new="AlgorithmConfig.num_cpus_per_learner",
            error=False,
        )
        self.num_cpus_per_learner = value

    @property
    @Deprecated(new="AlgorithmConfig.num_gpus_per_learner", error=False)
    def num_gpus_per_learner_worker(self):
        return self.num_gpus_per_learner

    @num_gpus_per_learner_worker.setter
    def num_gpus_per_learner_worker(self, value):
        deprecation_warning(
            old="AlgorithmConfig.num_gpus_per_learner_worker",
            new="AlgorithmConfig.num_gpus_per_learner",
            error=False,
        )
        self.num_gpus_per_learner = value

    @property
    @Deprecated(new="AlgorithmConfig.num_cpus_for_local_worker", error=False)
    def num_cpus_for_local_worker(self):
        return self.num_cpus_for_main_process

    @num_cpus_for_local_worker.setter
    def num_cpus_for_local_worker(self, value):
        deprecation_warning(
            old="AlgorithmConfig.num_cpus_for_local_worker",
            new="AlgorithmConfig.num_cpus_for_main_process",
            error=False,
        )
        self.num_cpus_for_main_process = value


class TorchCompileWhatToCompile(str, Enum):
    """Enumerates schemes of what parts of the TorchLearner can be compiled.

    This can be either the entire update step of the learner or only the forward
    methods (and therein the forward_train method) of the RLModule.

    .. note::
        - torch.compiled code can become slow on graph breaks or even raise
            errors on unsupported operations. Empirically, compiling
            `forward_train` should introduce little graph breaks, raise no
            errors but result in a speedup comparable to compiling the
            complete update.
        - Using `complete_update` is experimental and may result in errors.
    """

    # Compile the entire update step of the learner.
    # This includes the forward pass of the RLModule, the loss computation, and the
    # optimizer step.
    COMPLETE_UPDATE = "complete_update"
    # Only compile the forward methods (and therein the forward_train method) of the
    # RLModule.
    FORWARD_TRAIN = "forward_train"

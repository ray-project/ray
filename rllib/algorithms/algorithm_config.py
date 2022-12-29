import copy
import logging
import math
import os
from typing import (
    Any,
    Callable,
    Container,
    Dict,
    Optional,
    Tuple,
    Type,
    TYPE_CHECKING,
    Union,
)

import ray
from ray.rllib.evaluation.rollout_worker import RolloutWorker
from ray.rllib.algorithms.callbacks import DefaultCallbacks
from ray.rllib.env.env_context import EnvContext
from ray.rllib.env.multi_agent_env import MultiAgentEnv
from ray.rllib.evaluation.collectors.sample_collector import SampleCollector
from ray.rllib.evaluation.collectors.simple_list_collector import SimpleListCollector
from ray.rllib.evaluation.episode import Episode
from ray.rllib.env.wrappers.atari_wrappers import is_atari
from ray.rllib.models import MODEL_DEFAULTS
from ray.rllib.policy.policy import Policy, PolicySpec
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID
from ray.rllib.utils import deep_update, merge_dicts
from ray.rllib.utils.annotations import (
    OverrideToImplementCustomLogic_CallToSuperRecommended,
    ExperimentalAPI,
)
from ray.rllib.utils.deprecation import (
    Deprecated,
    DEPRECATED_VALUE,
    deprecation_warning,
)
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.from_config import from_config
from ray.rllib.utils.gym import (
    convert_old_gym_space_to_gymnasium_space,
    try_import_gymnasium_and_gym,
)
from ray.rllib.utils.policy import validate_policy_id
from ray.rllib.utils.typing import (
    AgentID,
    AlgorithmConfigDict,
    EnvConfigDict,
    EnvType,
    MultiAgentPolicyConfigDict,
    PartialAlgorithmConfigDict,
    PolicyID,
    ResultDict,
    SampleBatchType,
)
from ray.tune.logger import Logger
from ray.tune.registry import get_trainable_cls
from ray.tune.result import TRIAL_INFO
from ray.util import log_once

gym, old_gym = try_import_gymnasium_and_gym()
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
    from ray.rllib.core.rl_module import RLModule

logger = logging.getLogger(__name__)


class _NotProvided:
    """Singleton class to provide a "not provided" value for AlgorithmConfig signatures.

    Using the only instance of this class indicates that the user does NOT wish to
    change the value of some property.

    Examples:
        >>> from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
        >>> config = AlgorithmConfig()
        >>> # Print out the default learning rate.
        >>> print(config.lr)
        ... 0.001
        >>> # Print out the default `preprocessor_pref`.
        >>> print(config.preprocessor_pref)
        ... "deepmind"
        >>> # Will only set the `preprocessor_pref` property (to None) and leave
        >>> # all other properties at their default values.
        >>> config.training(preprocessor_pref=None)
        >>> config.preprocessor_pref is None
        ... True
        >>> # Still the same value (didn't touch it in the call to `.training()`.
        >>> print(config.lr)
        ... 0.001
    """

    class __NotProvided:
        pass

    instance = None

    def __init__(self):
        if _NotProvided.instance is None:
            _NotProvided.instance = _NotProvided.__NotProvided()


# Use this object as default values in all method signatures of
# AlgorithmConfig, indicating that the respective property should NOT be touched
# in the call.
NotProvided = _NotProvided()


# TODO (Kourosh): Move this to rllib.utils.importlib
def _resolve_class_path(module) -> Type:
    """Resolves a class path to a class.

    If the given module is already a class, it is returned as is.
    If the given module is a string, it is imported and the class is returned
    """
    if isinstance(module, Type):
        return module

    if isinstance(module, str):
        import importlib

        module_path, class_name = module.rsplit(".", 1)
        module = importlib.import_module(module_path)
        return getattr(module, class_name)


class AlgorithmConfig:
    """A RLlib AlgorithmConfig builds an RLlib Algorithm from a given configuration.

    Example:
        >>> from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
        >>> from ray.rllib.algorithms.callbacks import MemoryTrackingCallbacks
        >>> # Construct a generic config object, specifying values within different
        >>> # sub-categories, e.g. "training".
        >>> config = AlgorithmConfig().training(gamma=0.9, lr=0.01)  # doctest: +SKIP
        ...     .environment(env="CartPole-v1")
        ...     .resources(num_gpus=0)
        ...     .rollouts(num_rollout_workers=4)
        ...     .callbacks(MemoryTrackingCallbacks)
        >>> # A config object can be used to construct the respective Trainer.
        >>> rllib_algo = config.build()  # doctest: +SKIP

    Example:
        >>> from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
        >>> from ray import tune
        >>> # In combination with a tune.grid_search:
        >>> config = AlgorithmConfig()
        >>> config.training(lr=tune.grid_search([0.01, 0.001])) # doctest: +SKIP
        >>> # Use `to_dict()` method to get the legacy plain python config dict
        >>> # for usage with `tune.Tuner().fit()`.
        >>> tune.Tuner(  # doctest: +SKIP
        ...     "[registered trainer class]", param_space=config.to_dict()
        ...     ).fit()
    """

    @classmethod
    def from_dict(cls, config_dict: dict) -> "AlgorithmConfig":
        """Creates an AlgorithmConfig from a legacy python config dict.

        Examples:
            >>> from ray.rllib.algorithms.ppo.ppo import DEFAULT_CONFIG, PPOConfig
            >>> ppo_config = PPOConfig.from_dict(DEFAULT_CONFIG)
            >>> ppo = ppo_config.build(env="Pendulum-v1")

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

        Examples:
            >>> from ray.rllib.algorithms.ppo import PPOConfig
            >>> from ray.rllib.policy.policy import PolicySpec
            >>> config = (
            ...     PPOConfig()
            ...     .multi_agent(
            ...         policies={
            ...             "pol0": PolicySpec(config=PPOConfig.overrides(lambda_=0.95))
            ...         },
            ...     )
            ... )

            >>> from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
            >>> from ray.rllib.algorithms.pg import PGConfig
            >>> config = (
            ...     PGConfig()
            ...     .evaluation(
            ...         evaluation_num_workers=1,
            ...         evaluation_interval=1,
            ...         evaluation_config=AlgorithmConfig.overrides(explore=False),
            ...     )
            ... )

        Returns:
            A dict mapping valid config property-names to values.

        Raises:
            KeyError: In case a non-existing property name (kwargs key) is being
            passed in. Valid property names are taken from a default AlgorithmConfig
            object of `cls`.
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

    def __init__(self, algo_class=None):
        # Define all settings and their default values.

        # Define the default RLlib Trainer class that this AlgorithmConfig will be
        # applied to.
        self.algo_class = algo_class

        # `self.python_environment()`
        self.extra_python_environs_for_driver = {}
        self.extra_python_environs_for_worker = {}

        # `self.resources()`
        self.num_gpus = 0
        self.num_cpus_per_worker = 1
        self.num_gpus_per_worker = 0
        self._fake_gpus = False
        self.num_cpus_for_local_worker = 1
        self.custom_resources_per_worker = {}
        self.placement_strategy = "PACK"

        # `self.framework()`
        self.framework_str = "tf"
        self.eager_tracing = False
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

        # `self.environment()`
        self.env = None
        self.env_config = {}
        self.observation_space = None
        self.action_space = None
        self.env_task_fn = None
        self.render_env = False
        self.clip_rewards = None
        self.normalize_actions = True
        self.clip_actions = False
        self.disable_env_checking = False
        # Whether this env is an atari env (for atari-specific preprocessing).
        # If not specified, we will try to auto-detect this.
        self.is_atari = None

        # `self.rollouts()`
        self.num_rollout_workers = 0
        self.num_envs_per_worker = 1
        self.sample_collector = SimpleListCollector
        self.create_env_on_local_worker = False
        self.sample_async = False
        self.enable_connectors = False
        self.rollout_fragment_length = 200
        self.batch_mode = "truncate_episodes"
        self.remote_worker_envs = False
        self.remote_env_batch_wait_ms = 0
        self.validate_workers_after_construction = True
        self.ignore_worker_failures = False
        self.recreate_failed_workers = False
        self.restart_failed_sub_environments = False
        self.num_consecutive_worker_failures_tolerance = 100
        self.preprocessor_pref = "deepmind"
        self.observation_filter = "NoFilter"
        self.synchronize_filters = True
        self.compress_observations = False
        self.enable_tf1_exec_eagerly = False
        self.sampler_perf_stats_ema_coef = None

        # `self.training()`
        self.gamma = 0.99
        self.lr = 0.001
        self.train_batch_size = 32
        self.model = copy.deepcopy(MODEL_DEFAULTS)
        self.optimizer = {}
        self.max_requests_in_flight_per_sampler_worker = 2

        # `self.callbacks()`
        self.callbacks_class = DefaultCallbacks

        # `self.explore()`
        self.explore = True
        self.exploration_config = {
            # The Exploration class to use. In the simplest case, this is the name
            # (str) of any class present in the `rllib.utils.exploration` package.
            # You can also provide the python class directly or the full location
            # of your class (e.g. "ray.rllib.utils.exploration.epsilon_greedy.
            # EpsilonGreedy").
            "type": "StochasticSampling",
            # Add constructor kwargs here (if any).
        }

        # `self.multi_agent()`
        self.policies = {DEFAULT_POLICY_ID: PolicySpec()}
        self.policy_map_capacity = 100
        self.policy_mapping_fn = (
            lambda aid, episode, worker, **kwargs: DEFAULT_POLICY_ID
        )
        self.policies_to_train = None
        self.policy_states_are_swappable = False
        self.observation_fn = None
        self.count_steps_by = "env_steps"

        # `self.offline_data()`
        self.input_ = "sampler"
        self.input_config = {}
        self.actions_in_input_normalized = False
        self.postprocess_inputs = False
        self.shuffle_buffer_size = 0
        self.output = None
        self.output_config = {}
        self.output_compress_columns = ["obs", "new_obs"]
        self.output_max_file_size = 64 * 1024 * 1024
        self.offline_sampling = False

        # `self.evaluation()`
        self.evaluation_interval = None
        self.evaluation_duration = 10
        self.evaluation_duration_unit = "episodes"
        self.evaluation_sample_timeout_s = 180.0
        self.evaluation_parallel_to_training = False
        self.evaluation_config = None
        self.off_policy_estimation_methods = {}
        self.ope_split_batch_by_episode = True
        self.evaluation_num_workers = 0
        self.custom_evaluation_function = None
        self.always_attach_evaluation_results = False
        self.enable_async_evaluation = False
        # TODO: Set this flag still in the config or - much better - in the
        #  RolloutWorker as a property.
        self.in_evaluation = False
        self.sync_filters_on_rollout_workers_timeout_s = 60.0

        # `self.reporting()`
        self.keep_per_episode_custom_metrics = False
        self.metrics_episode_collection_timeout_s = 60.0
        self.metrics_num_episodes_for_smoothing = 100
        self.min_time_s_per_iteration = None
        self.min_train_timesteps_per_iteration = 0
        self.min_sample_timesteps_per_iteration = 0

        # `self.checkpointing()`
        self.export_native_model_files = False
        self.checkpoint_trainable_policies_only = False

        # `self.debugging()`
        self.logger_creator = None
        self.logger_config = None
        self.log_level = DEPRECATED_VALUE
        self.log_sys_usage = True
        self.fake_sampler = False
        self.seed = None
        self.worker_cls = None

        # `self.rl_module()`
        self.rl_module_class = None
        self._enable_rl_module_api = False

        # `self.experimental()`
        self._tf_policy_handles_more_than_one_loss = False
        self._disable_preprocessor_api = False
        self._disable_action_flattening = False
        self._disable_execution_plan_api = True

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

        # The following values have moved because of the new ReplayBuffer API
        self.buffer_size = DEPRECATED_VALUE
        self.capacity = DEPRECATED_VALUE
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
        self.horizon = DEPRECATED_VALUE
        self.soft_horizon = DEPRECATED_VALUE
        self.no_done_at_end = DEPRECATED_VALUE

    def to_dict(self) -> AlgorithmConfigDict:
        """Converts all settings into a legacy config dict for backward compatibility.

        Returns:
            A complete AlgorithmConfigDict, usable in backward-compatible Tune/RLlib
            use cases, e.g. w/ `tune.Tuner().fit()`.
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

        # Setup legacy multi-agent sub-dict:
        config["multiagent"] = {}
        for k in self.multiagent.keys():
            # convert policies dict to something human-readable
            if k == "policies" and isinstance(self.multiagent[k], dict):
                policies_dict = {}
                for policy_id, policy_spec in self.multiagent[k].items():
                    if isinstance(policy_spec, PolicySpec):
                        policies_dict[policy_id] = (
                            policy_spec.policy_class,
                            policy_spec.observation_space,
                            policy_spec.action_space,
                            policy_spec.config,
                        )
                    else:
                        policies_dict[policy_id] = policy_spec
                config["multiagent"][k] = policies_dict
            else:
                config["multiagent"][k] = config.pop(k)

        # Switch out deprecated vs new config keys.
        config["callbacks"] = config.pop("callbacks_class", DefaultCallbacks)
        config["create_env_on_driver"] = config.pop("create_env_on_local_worker", 1)
        config["custom_eval_function"] = config.pop("custom_evaluation_function", None)
        config["framework"] = config.pop("framework_str", None)
        config["num_cpus_for_driver"] = config.pop("num_cpus_for_local_worker", 1)
        config["num_workers"] = config.pop("num_rollout_workers", 0)

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

        # Modify our properties one by one.
        for key, value in config_dict.items():
            key = self._translate_special_keys(key, warn_deprecated=False)

            # Ray Tune saves additional data under this magic keyword.
            # This should not get treated as AlgorithmConfig field.
            if key == TRIAL_INFO:
                continue

            # Set our multi-agent settings.
            if key == "multiagent":
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
            elif key == "callbacks_class":
                self.callbacks(callbacks_class=value)
            elif key == "env_config":
                self.environment(env_config=value)
            elif key.startswith("evaluation_"):
                eval_call[key] = value
            elif key == "exploration_config":
                self.exploration(exploration_config=value)
            elif key in ["model", "optimizer", "replay_buffer_config"]:
                self.training(**{key: value})
            # If config key matches a property, just set it, otherwise, warn and set.
            else:
                if not hasattr(self, key) and log_once(
                    "unknown_property_in_algo_config"
                ):
                    logger.warning(
                        f"Cannot create {type(self).__name__} from given "
                        f"`config_dict`! Property {key} not supported."
                    )
                setattr(self, key, value)

        self.evaluation(**eval_call)

        return self

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

    def _detect_atari_env(self) -> bool:
        """Returns whether this configured env is an Atari env or not.

        Returns:
            True, if specified env is an Atari env, False otherwise.
        """
        # Atari envs are usually specified via a string like "PongNoFrameskip-v4"
        # or "ALE/Breakout-v5".
        # We do NOT attempt to auto-detect Atari env for other specified types like
        # a callable, to avoid running heavy logics in validate().
        # For these cases, users can explicitly set `environment(atari=True)`.
        if not type(self.env) == str:
            return False

        try:
            if self.env.startswith("ALE/"):
                env = gym.make("GymV26Environment-v0", env_id=self.env)
            else:
                env = gym.make(self.env)
        except gym.error.NameNotFound:
            # Not an Atari env if this is not a gym env.
            return False

        return is_atari(env)

    @OverrideToImplementCustomLogic_CallToSuperRecommended
    def validate(self) -> None:
        """Validates all values in this config.

        Note: This should NOT include immediate checks on single value
        correctness, e.g. "batch_mode" = [complete_episodes|truncate_episodes].
        Those simgular, independent checks should instead go directly into their
        respective methods.
        """
        # Check correct framework settings, and whether configured framework is
        # installed.
        _tf1, _tf, _tfv = None, None, None
        _torch = None
        if self.framework_str not in {"tf", "tf2"} and self.framework_str != "torch":
            return
        elif self.framework_str in {"tf", "tf2"}:
            _tf1, _tf, _tfv = try_import_tf()
        else:
            _torch, _ = try_import_torch()

        self._check_if_correct_nn_framework_installed(_tf1, _tf, _torch)
        self._resolve_tf_settings(_tf1, _tfv)

        # Check `policies_to_train` for invalid entries.
        if isinstance(self.policies_to_train, (list, set, tuple)):
            for pid in self.policies_to_train:
                if pid not in self.policies:
                    raise ValueError(
                        "`config.multi_agent(policies_to_train=..)` contains "
                        f"policy ID ({pid}) that was not defined in "
                        f"`config.multi_agent(policies=..)`!"
                    )

        # If `evaluation_num_workers` > 0, warn if `evaluation_interval` is
        # None.
        if self.evaluation_num_workers > 0 and not self.evaluation_interval:
            logger.warning(
                f"You have specified {self.evaluation_num_workers} "
                "evaluation workers, but your `evaluation_interval` is None! "
                "Therefore, evaluation will not occur automatically with each"
                " call to `Algorithm.train()`. Instead, you will have to call "
                "`Algorithm.evaluate()` manually in order to trigger an "
                "evaluation run."
            )
        # If `evaluation_num_workers=0` and
        # `evaluation_parallel_to_training=True`, warn that you need
        # at least one remote eval worker for parallel training and
        # evaluation, and set `evaluation_parallel_to_training` to False.
        elif self.evaluation_num_workers == 0 and self.evaluation_parallel_to_training:
            raise ValueError(
                "`evaluation_parallel_to_training` can only be done if "
                "`evaluation_num_workers` > 0! Try setting "
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
        # Make sure, it's an int otherwise.
        elif (
            not isinstance(self.evaluation_duration, int)
            or self.evaluation_duration <= 0
        ):
            raise ValueError(
                f"`evaluation_duration` ({self.evaluation_duration}) must be an "
                f"int and >0!"
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

        # RLModule API only works with connectors.
        if not self.enable_connectors and self._enable_rl_module_api:
            raise ValueError(
                "RLModule API only works with connectors. "
                "Please enable connectors via "
                "`config.rollouts(enable_connectors=True)`."
            )

        if bool(os.environ.get("RLLIB_ENABLE_RL_MODULE", False)):
            # enable RLModule API and connectors if env variable is set
            # (to be used in unittesting)
            self._enable_rl_module_api = True
            self.enable_connectors = True

        # TODO: Deprecate self.simple_optimizer!
        # Multi-GPU settings.
        if self.simple_optimizer is True:
            pass
        # Multi-GPU setting: Must use MultiGPUTrainOneStep.
        elif self.num_gpus > 1:
            # TODO: AlphaStar uses >1 GPUs differently (1 per policy actor), so this is
            #  ok for tf2 here.
            #  Remove this hacky check, once we have fully moved to the RLTrainer API.
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

        # Detect if specified env is an Atari env.
        if self.is_atari is None:
            self.is_atari = self._detect_atari_env()

        if self.input_ == "sampler" and self.off_policy_estimation_methods:
            raise ValueError(
                "Off-policy estimation methods can only be used if the input is a "
                "dataset. We currently do not support applying off_policy_esitmation "
                "method on a sampler input."
            )

        if self.input_ == "dataset":
            # if we need to read a ray dataset set the parallelism and
            # num_cpus_per_read_task from rollout worker settings
            self.input_config["num_cpus_per_read_task"] = self.num_cpus_per_worker
            if self.in_evaluation:
                # If using dataset for evaluation, the parallelism gets set to
                # evaluation_num_workers for backward compatibility and num_cpus gets
                # set to num_cpus_per_worker from rollout worker. User only needs to
                # set evaluation_num_workers.
                self.input_config["parallelism"] = self.evaluation_num_workers or 1
            else:
                # If using dataset for training, the parallelism and num_cpus gets set
                # based on rollout worker parameters. This is for backwards
                # compatibility for now. User only needs to set num_rollout_workers.
                self.input_config["parallelism"] = self.num_rollout_workers or 1

        # resolve rl_module class
        if self._enable_rl_module_api and self.rl_module_class is None:
            rl_module_class_path = self.get_default_rl_module_class()
            self.rl_module_class = _resolve_class_path(rl_module_class_path)

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
                "ray.rllib.examples.env.random_env.RandomEnv"), or an Env
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
        num_gpus: Optional[Union[float, int]] = NotProvided,
        _fake_gpus: Optional[bool] = NotProvided,
        num_cpus_per_worker: Optional[Union[float, int]] = NotProvided,
        num_gpus_per_worker: Optional[Union[float, int]] = NotProvided,
        num_cpus_for_local_worker: Optional[int] = NotProvided,
        custom_resources_per_worker: Optional[dict] = NotProvided,
        placement_strategy: Optional[str] = NotProvided,
    ) -> "AlgorithmConfig":
        """Specifies resources allocated for an Algorithm and its ray actors/workers.

        Args:
            num_gpus: Number of GPUs to allocate to the algorithm process.
                Note that not all algorithms can take advantage of GPUs.
                Support for multi-GPU is currently only available for
                tf-[PPO/IMPALA/DQN/PG]. This can be fractional (e.g., 0.3 GPUs).
            _fake_gpus: Set to True for debugging (multi-)?GPU funcitonality on a
                CPU machine. GPU towers will be simulated by graphs located on
                CPUs in this case. Use `num_gpus` to test for different numbers of
                fake GPUs.
            num_cpus_per_worker: Number of CPUs to allocate per worker.
            num_gpus_per_worker: Number of GPUs to allocate per worker. This can be
                fractional. This is usually needed only if your env itself requires a
                GPU (i.e., it is a GPU-intensive video game), or model inference is
                unusually expensive.
            custom_resources_per_worker: Any custom Ray resources to allocate per
                worker.
            num_cpus_for_local_worker: Number of CPUs to allocate for the algorithm.
                Note: this only takes effect when running in Tune. Otherwise,
                the algorithm runs in the main program (driver).
            custom_resources_per_worker: Any custom Ray resources to allocate per
                worker.
            placement_strategy: The strategy for the placement group factory returned by
                `Algorithm.default_resource_request()`. A PlacementGroup defines, which
                devices (resources) should always be co-located on the same node.
                For example, an Algorithm with 2 rollout workers, running with
                num_gpus=1 will request a placement group with the bundles:
                [{"gpu": 1, "cpu": 1}, {"cpu": 1}, {"cpu": 1}], where the first bundle
                is for the driver and the other 2 bundles are for the two workers.
                These bundles can now be "placed" on the same or different
                nodes depending on the value of `placement_strategy`:
                "PACK": Packs bundles into as few nodes as possible.
                "SPREAD": Places bundles across distinct nodes as even as possible.
                "STRICT_PACK": Packs bundles into one node. The group is not allowed
                to span multiple nodes.
                "STRICT_SPREAD": Packs bundles across distinct nodes.

        Returns:
            This updated AlgorithmConfig object.
        """
        if num_gpus is not NotProvided:
            self.num_gpus = num_gpus
        if _fake_gpus is not NotProvided:
            self._fake_gpus = _fake_gpus
        if num_cpus_per_worker is not NotProvided:
            self.num_cpus_per_worker = num_cpus_per_worker
        if num_gpus_per_worker is not NotProvided:
            self.num_gpus_per_worker = num_gpus_per_worker
        if num_cpus_for_local_worker is not NotProvided:
            self.num_cpus_for_local_worker = num_cpus_for_local_worker
        if custom_resources_per_worker is not NotProvided:
            self.custom_resources_per_worker = custom_resources_per_worker
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
    ) -> "AlgorithmConfig":
        """Sets the config's DL framework settings.

        Args:
            framework: tf: TensorFlow (static-graph); tf2: TensorFlow 2.x
                (eager or traced, if eager_tracing=True); torch: PyTorch
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
    ) -> "AlgorithmConfig":
        """Sets the config's RL-environment settings.

        Args:
            env: The environment specifier. This can either be a tune-registered env,
                via `tune.register_env([name], lambda env_ctx: [env object])`,
                or a string specifier of an RLlib supported type. In the latter case,
                RLlib will try to interpret the specifier as either an openAI gym env,
                a PyBullet env, a ViZDoomGym env, or a fully qualified classpath to an
                Env class, e.g. "ray.rllib.examples.env.random_env.RandomEnv".
            env_config: Arguments dict passed to the env creator as an EnvContext
                object (which is a dict plus the properties: num_rollout_workers,
                worker_index, vector_index, and remote).
            observation_space: The observation space for the Policies of this Algorithm.
            action_space: The action space for the Policies of this Algorithm.
            env_task_fn: A callable taking the last train results, the base env and the
                env context as args and returning a new task to set the env to.
                The env must be a `TaskSettableEnv` sub-class for this to work.
                See `examples/curriculum_learning.py` for an example.
            render_env: If True, try to render the environment on the local worker or on
                worker 1 (if num_rollout_workers > 0). For vectorized envs, this usually
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
                components). We will unsquash actions (and clip, just in case) to the
                bounds of the env's action space before sending actions back to the env.
            clip_actions: If True, RLlib will clip actions according to the env's bounds
                before sending them back to the env.
                TODO: (sven) This option should be deprecated and always be False.
            disable_env_checking: If True, disable the environment pre-checking module.
            is_atari: This config can be used to explicitly specify whether the env is
                an Atari env or not. If not specified, RLlib will try to auto-detect
                this during config validation.

        Returns:
            This updated AlgorithmConfig object.
        """
        if env is not NotProvided:
            self.env = env
        if env_config is not NotProvided:
            deep_update(
                self.env_config,
                env_config,
                True,
            )
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
            self.is_atari = is_atari

        return self

    def rollouts(
        self,
        *,
        num_rollout_workers: Optional[int] = NotProvided,
        num_envs_per_worker: Optional[int] = NotProvided,
        create_env_on_local_worker: Optional[bool] = NotProvided,
        sample_collector: Optional[Type[SampleCollector]] = NotProvided,
        sample_async: Optional[bool] = NotProvided,
        enable_connectors: Optional[bool] = NotProvided,
        rollout_fragment_length: Optional[Union[int, str]] = NotProvided,
        batch_mode: Optional[str] = NotProvided,
        remote_worker_envs: Optional[bool] = NotProvided,
        remote_env_batch_wait_ms: Optional[float] = NotProvided,
        validate_workers_after_construction: Optional[bool] = NotProvided,
        ignore_worker_failures: Optional[bool] = NotProvided,
        recreate_failed_workers: Optional[bool] = NotProvided,
        restart_failed_sub_environments: Optional[bool] = NotProvided,
        num_consecutive_worker_failures_tolerance: Optional[int] = NotProvided,
        preprocessor_pref: Optional[str] = NotProvided,
        observation_filter: Optional[str] = NotProvided,
        synchronize_filter: Optional[bool] = NotProvided,
        compress_observations: Optional[bool] = NotProvided,
        enable_tf1_exec_eagerly: Optional[bool] = NotProvided,
        sampler_perf_stats_ema_coef: Optional[float] = NotProvided,
        horizon=DEPRECATED_VALUE,
        soft_horizon=DEPRECATED_VALUE,
        no_done_at_end=DEPRECATED_VALUE,
    ) -> "AlgorithmConfig":
        """Sets the rollout worker configuration.

        Args:
            num_rollout_workers: Number of rollout worker actors to create for
                parallel sampling. Setting this to 0 will force rollouts to be done in
                the local worker (driver process or the Algorithm's actor when using
                Tune).
            num_envs_per_worker: Number of environments to evaluate vector-wise per
                worker. This enables model inference batching, which can improve
                performance for inference bottlenecked workloads.
            sample_collector: The SampleCollector class to be used to collect and
                retrieve environment-, model-, and sampler data. Override the
                SampleCollector base class to implement your own
                collection/buffering/retrieval logic.
            create_env_on_local_worker: When `num_rollout_workers` > 0, the driver
                (local_worker; worker-idx=0) does not need an environment. This is
                because it doesn't have to sample (done by remote_workers;
                worker_indices > 0) nor evaluate (done by evaluation workers;
                see below).
            sample_async: Use a background thread for sampling (slightly off-policy,
                usually not advisable to turn on unless your env specifically requires
                it).
            enable_connectors: Use connector based environment runner, so that all
                preprocessing of obs and postprocessing of actions are done in agent
                and action connectors.
            rollout_fragment_length: Divide episodes into fragments of this many steps
                each during rollouts. Trajectories of this size are collected from
                rollout workers and combined into a larger batch of `train_batch_size`
                for learning.
                For example, given rollout_fragment_length=100 and
                train_batch_size=1000:
                1. RLlib collects 10 fragments of 100 steps each from rollout workers.
                2. These fragments are concatenated and we perform an epoch of SGD.
                When using multiple envs per worker, the fragment size is multiplied by
                `num_envs_per_worker`. This is since we are collecting steps from
                multiple envs in parallel. For example, if num_envs_per_worker=5, then
                rollout workers will return experiences in chunks of 5*100 = 500 steps.
                The dataflow here can vary per algorithm. For example, PPO further
                divides the train batch into minibatches for multi-epoch SGD.
                Set to "auto" to have RLlib compute an exact `rollout_fragment_length`
                to match the given batch size.
            batch_mode: How to build per-Sampler (RolloutWorker) batches, which are then
                usually concat'd to form the train batch. Note that "steps" below can
                mean different things (either env- or agent-steps) and depends on the
                `count_steps_by` setting, adjustable via
                `AlgorithmConfig.multi_agent(count_steps_by=..)`:
                1) "truncate_episodes": Each call to sample() will return a
                batch of at most `rollout_fragment_length * num_envs_per_worker` in
                size. The batch will be exactly `rollout_fragment_length * num_envs`
                in size if postprocessing does not change batch sizes. Episodes
                may be truncated in order to meet this size requirement.
                This mode guarantees evenly sized batches, but increases
                variance as the future return must now be estimated at truncation
                boundaries.
                2) "complete_episodes": Each call to sample() will return a
                batch of at least `rollout_fragment_length * num_envs_per_worker` in
                size. Episodes will not be truncated, but multiple episodes
                may be packed within one batch to meet the (minimum) batch size.
                Note that when `num_envs_per_worker > 1`, episode steps will be buffered
                until the episode completes, and hence batches may contain
                significant amounts of off-policy data.
            remote_worker_envs: If using num_envs_per_worker > 1, whether to create
                those new envs in remote processes instead of in the same worker.
                This adds overheads, but can make sense if your envs can take much
                time to step / reset (e.g., for StarCraft). Use this cautiously;
                overheads are significant.
            remote_env_batch_wait_ms: Timeout that remote workers are waiting when
                polling environments. 0 (continue when at least one env is ready) is
                a reasonable default, but optimal value could be obtained by measuring
                your environment step / reset and model inference perf.
            validate_workers_after_construction: Whether to validate that each created
                remote worker is healthy after its construction process.
            ignore_worker_failures: Whether to attempt to continue training if a worker
                crashes. The number of currently healthy workers is reported as the
                "num_healthy_workers" metric.
            recreate_failed_workers: Whether - upon a worker failure - RLlib will try to
                recreate the lost worker as an identical copy of the failed one. The new
                worker will only differ from the failed one in its
                `self.recreated_worker=True` property value. It will have the same
                `worker_index` as the original one. If True, the
                `ignore_worker_failures` setting will be ignored.
            restart_failed_sub_environments: If True and any sub-environment (within
                a vectorized env) throws any error during env stepping, the
                Sampler will try to restart the faulty sub-environment. This is done
                without disturbing the other (still intact) sub-environment and without
                the RolloutWorker crashing.
            num_consecutive_worker_failures_tolerance: The number of consecutive times
                a rollout worker (or evaluation worker) failure is tolerated before
                finally crashing the Algorithm. Only useful if either
                `ignore_worker_failures` or `recreate_failed_workers` is True.
                Note that for `restart_failed_sub_environments` and sub-environment
                failures, the worker itself is NOT affected and won't throw any errors
                as the flawed sub-environment is silently restarted under the hood.
            preprocessor_pref: Whether to use "rllib" or "deepmind" preprocessors by
                default. Set to None for using no preprocessor. In this case, the
                model will have to handle possibly complex observations from the
                environment.
            observation_filter: Element-wise observation filter, either "NoFilter"
                or "MeanStdFilter".
            synchronize_filter: Whether to synchronize the statistics of remote filters.
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
        if num_rollout_workers is not NotProvided:
            self.num_rollout_workers = num_rollout_workers
        if num_envs_per_worker is not NotProvided:
            if num_envs_per_worker <= 0:
                raise ValueError(
                    f"`num_envs_per_worker` ({num_envs_per_worker}) must be "
                    f"larger than 0!"
                )
            self.num_envs_per_worker = num_envs_per_worker
        if sample_collector is not NotProvided:
            self.sample_collector = sample_collector
        if create_env_on_local_worker is not NotProvided:
            self.create_env_on_local_worker = create_env_on_local_worker
        if sample_async is not NotProvided:
            self.sample_async = sample_async
        if enable_connectors is not NotProvided:
            self.enable_connectors = enable_connectors
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

        # Check batching/sample collection settings.
        if batch_mode is not NotProvided:
            if batch_mode not in ["truncate_episodes", "complete_episodes"]:
                raise ValueError(
                    "`config.batch_mode` must be one of [truncate_episodes|"
                    "complete_episodes]! Got {}".format(batch_mode)
                )
            self.batch_mode = batch_mode

        if remote_worker_envs is not NotProvided:
            self.remote_worker_envs = remote_worker_envs
        if remote_env_batch_wait_ms is not NotProvided:
            self.remote_env_batch_wait_ms = remote_env_batch_wait_ms
        if validate_workers_after_construction is not NotProvided:
            self.validate_workers_after_construction = (
                validate_workers_after_construction
            )
        if ignore_worker_failures is not NotProvided:
            self.ignore_worker_failures = ignore_worker_failures
        if recreate_failed_workers is not NotProvided:
            self.recreate_failed_workers = recreate_failed_workers
        if restart_failed_sub_environments is not NotProvided:
            self.restart_failed_sub_environments = restart_failed_sub_environments
        if num_consecutive_worker_failures_tolerance is not NotProvided:
            self.num_consecutive_worker_failures_tolerance = (
                num_consecutive_worker_failures_tolerance
            )
        if preprocessor_pref is not NotProvided:
            assert preprocessor_pref in ("rllib", "deepmind", None)
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
        if horizon != DEPRECATED_VALUE:
            deprecation_warning(
                old="AlgorithmConfig.rollouts(horizon=..)",
                new="You should wrap your gymnasium.Env with a "
                "gymnasium.wrappers.TimeLimit wrapper.",
                error=True,
            )
        if soft_horizon != DEPRECATED_VALUE:
            deprecation_warning(
                old="AlgorithmConfig.rollouts(soft_horizon=..)",
                new="Your gymnasium.Env.step() should handle soft resets internally.",
                error=True,
            )
        if no_done_at_end != DEPRECATED_VALUE:
            deprecation_warning(
                old="AlgorithmConfig.rollouts(no_done_at_end=..)",
                new="Your gymnasium.Env.step() should return a truncated=True flag",
                error=True,
            )

        return self

    def training(
        self,
        gamma: Optional[float] = NotProvided,
        lr: Optional[float] = NotProvided,
        train_batch_size: Optional[int] = NotProvided,
        model: Optional[dict] = NotProvided,
        optimizer: Optional[dict] = NotProvided,
        max_requests_in_flight_per_sampler_worker: Optional[int] = NotProvided,
    ) -> "AlgorithmConfig":
        """Sets the training related configuration.

        Args:
            gamma: Float specifying the discount factor of the Markov Decision process.
            lr: The default learning rate.
            train_batch_size: Training batch size, if applicable.
            model: Arguments passed into the policy model. See models/catalog.py for a
                full list of the available model options.
                TODO: Provide ModelConfig objects instead of dicts.
            optimizer: Arguments to pass to the policy optimizer.
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

        Returns:
            This updated AlgorithmConfig object.
        """
        if gamma is not NotProvided:
            self.gamma = gamma
        if lr is not NotProvided:
            self.lr = lr
        if train_batch_size is not NotProvided:
            self.train_batch_size = train_batch_size
        if model is not NotProvided:
            # Validate prev_a/r settings.
            prev_a_r = model.get("lstm_use_prev_action_reward", DEPRECATED_VALUE)
            if prev_a_r != DEPRECATED_VALUE:
                deprecation_warning(
                    "model.lstm_use_prev_action_reward",
                    "model.lstm_use_prev_action and model.lstm_use_prev_reward",
                    error=True,
                )
            self.model.update(model)
            if (
                model.get("_use_default_native_models", DEPRECATED_VALUE)
                != DEPRECATED_VALUE
            ):
                deprecation_warning(
                    old="AlgorithmConfig.training(_use_default_native_models=True)",
                    help="_use_default_native_models is not supported "
                    "anymore. To get rid of this error, set `experimental("
                    "_enable_rl_module_api` to True. Native models will "
                    "be better supported by the upcoming RLModule API.",
                    # Error out if user tries to enable this
                    error=model["_use_default_native_models"],
                )

        if optimizer is not NotProvided:
            self.optimizer = merge_dicts(self.optimizer, optimizer)
        if max_requests_in_flight_per_sampler_worker is not NotProvided:
            self.max_requests_in_flight_per_sampler_worker = (
                max_requests_in_flight_per_sampler_worker
            )

        return self

    def callbacks(self, callbacks_class) -> "AlgorithmConfig":
        """Sets the callbacks configuration.

        Args:
            callbacks_class: Callbacks class, whose methods will be run during
                various phases of training and environment sample collection.
                See the `DefaultCallbacks` class and
                `examples/custom_metrics_and_callbacks.py` for more usage information.

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

    def exploration(
        self,
        *,
        explore: Optional[bool] = NotProvided,
        exploration_config: Optional[dict] = NotProvided,
    ) -> "AlgorithmConfig":
        """Sets the config's exploration settings.

        Args:
            explore: Default exploration behavior, iff `explore=None` is passed into
                compute_action(s). Set to False for no exploration behavior (e.g.,
                for evaluation).
            exploration_config: A dict specifying the Exploration object's config.

        Returns:
            This updated AlgorithmConfig object.
        """
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

        return self

    def evaluation(
        self,
        *,
        evaluation_interval: Optional[int] = NotProvided,
        evaluation_duration: Optional[Union[int, str]] = NotProvided,
        evaluation_duration_unit: Optional[str] = NotProvided,
        evaluation_sample_timeout_s: Optional[float] = NotProvided,
        evaluation_parallel_to_training: Optional[bool] = NotProvided,
        evaluation_config: Optional[
            Union["AlgorithmConfig", PartialAlgorithmConfigDict]
        ] = NotProvided,
        off_policy_estimation_methods: Optional[Dict] = NotProvided,
        ope_split_batch_by_episode: Optional[bool] = NotProvided,
        evaluation_num_workers: Optional[int] = NotProvided,
        custom_evaluation_function: Optional[Callable] = NotProvided,
        always_attach_evaluation_results: Optional[bool] = NotProvided,
        enable_async_evaluation: Optional[bool] = NotProvided,
        # Deprecated args.
        evaluation_num_episodes=DEPRECATED_VALUE,
    ) -> "AlgorithmConfig":
        """Sets the config's evaluation settings.

        Args:
            evaluation_interval: Evaluate with every `evaluation_interval` training
                iterations. The evaluation stats will be reported under the "evaluation"
                metric key. Note that for Ape-X metrics are already only reported for
                the lowest epsilon workers (least random workers).
                Set to None (or 0) for no evaluation.
            evaluation_duration: Duration for which to run evaluation each
                `evaluation_interval`. The unit for the duration can be set via
                `evaluation_duration_unit` to either "episodes" (default) or
                "timesteps". If using multiple evaluation workers
                (evaluation_num_workers > 1), the load to run will be split amongst
                these.
                If the value is "auto":
                - For `evaluation_parallel_to_training=True`: Will run as many
                episodes/timesteps that fit into the (parallel) training step.
                - For `evaluation_parallel_to_training=False`: Error.
            evaluation_duration_unit: The unit, with which to count the evaluation
                duration. Either "episodes" (default) or "timesteps".
            evaluation_sample_timeout_s: The timeout (in seconds) for the ray.get call
                to the remote evaluation worker(s) `sample()` method. After this time,
                the user will receive a warning and instructions on how to fix the
                issue. This could be either to make sure the episode ends, increasing
                the timeout, or switching to `evaluation_duration_unit=timesteps`.
            evaluation_parallel_to_training: Whether to run evaluation in parallel to
                a Algorithm.train() call using threading. Default=False.
                E.g. evaluation_interval=2 -> For every other training iteration,
                the Algorithm.train() and Algorithm.evaluate() calls run in parallel.
                Note: This is experimental. Possible pitfalls could be race conditions
                for weight synching at the beginning of the evaluation loop.
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
            evaluation_num_workers: Number of parallel workers to use for evaluation.
                Note that this is set to zero by default, which means evaluation will
                be run in the algorithm process (only if evaluation_interval is not
                None). If you increase this, it will increase the Ray resource usage of
                the algorithm since evaluation workers are created separately from
                rollout workers (used to sample data for training).
            custom_evaluation_function: Customize the evaluation method. This must be a
                function of signature (algo: Algorithm, eval_workers: WorkerSet) ->
                metrics: dict. See the Algorithm.evaluate() method to see the default
                implementation. The Algorithm guarantees all eval workers have the
                latest policy state before this function is called.
            always_attach_evaluation_results: Make sure the latest available evaluation
                results are always attached to a step result dict. This may be useful
                if Tune or some other meta controller needs access to evaluation metrics
                all the time.
            enable_async_evaluation: If True, use an AsyncRequestsManager for
                the evaluation workers and use this manager to send `sample()` requests
                to the evaluation workers. This way, the Algorithm becomes more robust
                against long running episodes and/or failing (and restarting) workers.

        Returns:
            This updated AlgorithmConfig object.
        """
        if evaluation_num_episodes != DEPRECATED_VALUE:
            deprecation_warning(
                old="AlgorithmConfig.evaluation(evaluation_num_episodes=..)",
                new="AlgorithmConfig.evaluation(evaluation_duration=.., "
                "evaluation_duration_unit='episodes')",
                error=False,
            )
            evaluation_duration = evaluation_num_episodes

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
        if evaluation_num_workers is not NotProvided:
            self.evaluation_num_workers = evaluation_num_workers
        if custom_evaluation_function is not NotProvided:
            self.custom_evaluation_function = custom_evaluation_function
        if always_attach_evaluation_results is not NotProvided:
            self.always_attach_evaluation_results = always_attach_evaluation_results
        if enable_async_evaluation is not NotProvided:
            self.enable_async_evaluation = enable_async_evaluation
        if ope_split_batch_by_episode is not NotProvided:
            self.ope_split_batch_by_episode = ope_split_batch_by_episode

        return self

    def offline_data(
        self,
        *,
        input_=NotProvided,
        input_config=NotProvided,
        actions_in_input_normalized=NotProvided,
        input_evaluation=NotProvided,
        postprocess_inputs=NotProvided,
        shuffle_buffer_size=NotProvided,
        output=NotProvided,
        output_config=NotProvided,
        output_compress_columns=NotProvided,
        output_max_file_size=NotProvided,
        offline_sampling=NotProvided,
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
                ray.rllib.offline.InputReader.
                - A string key that indexes a callable with tune.registry.register_input
            input_config: Arguments that describe the settings for reading the input.
                If input is `sample`, this will be environment configuation, e.g.
                `env_name` and `env_config`, etc. See `EnvContext` for more info.
                If the input is `dataset`, this will be e.g. `format`, `path`.
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
                output data.
            output_max_file_size: Max output file size (in bytes) before rolling over
                to a new file.
            offline_sampling: Whether sampling for the Algorithm happens via
                reading from offline data. If True, RolloutWorkers will NOT limit the
                number of collected batches within the same `sample()` call based on
                the number of sub-environments within the worker (no sub-environments
                present).

        Returns:
            This updated AlgorithmConfig object.
        """
        if input_ is not NotProvided:
            self.input_ = input_
        if input_config is not NotProvided:
            if not isinstance(input_config, dict):
                raise ValueError(
                    f"input_config must be a dict, got {type(input_config)}."
                )
            # TODO (Kourosh) Once we use a complete sepration between rollout worker
            # and input dataset reader we can remove this.
            # For now Error out if user attempts to set these parameters.
            msg = "{} should not be set in the input_config. RLlib will use {} instead."
            if input_config.get("num_cpus_per_read_task") is not None:
                raise ValueError(
                    msg.format(
                        "num_cpus_per_read_task",
                        "config.resources(num_cpus_per_worker=..)",
                    )
                )
            if input_config.get("parallelism") is not None:
                if self.in_evaluation:
                    raise ValueError(
                        msg.format(
                            "parallelism",
                            "config.evaluation(evaluation_num_workers=..)",
                        )
                    )
                else:
                    raise ValueError(
                        msg.format(
                            "parallelism", "config.rollouts(num_rollout_workers=..)"
                        )
                    )
            self.input_config = input_config
        if actions_in_input_normalized is not NotProvided:
            self.actions_in_input_normalized = actions_in_input_normalized
        if input_evaluation is not NotProvided:
            deprecation_warning(
                old="offline_data(input_evaluation={})".format(input_evaluation),
                new="evaluation(off_policy_estimation_methods={})".format(
                    input_evaluation
                ),
                error=True,
                help="Running OPE during training is not recommended.",
            )
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
        if offline_sampling is not NotProvided:
            self.offline_sampling = offline_sampling

        return self

    def multi_agent(
        self,
        *,
        policies=NotProvided,
        policy_map_capacity: Optional[int] = NotProvided,
        policy_mapping_fn: Optional[
            Callable[[AgentID, "Episode"], PolicyID]
        ] = NotProvided,
        policies_to_train: Optional[
            Union[Container[PolicyID], Callable[[PolicyID, SampleBatchType], bool]]
        ] = NotProvided,
        policy_states_are_swappable: Optional[bool] = NotProvided,
        observation_fn: Optional[Callable] = NotProvided,
        count_steps_by: Optional[str] = NotProvided,
        # Deprecated args:
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
                validate_policy_id(pid, error=True)

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

        if policy_map_capacity is not NotProvided:
            self.policy_map_capacity = policy_map_capacity

        if policy_mapping_fn is not NotProvided:
            # Attempt to create a `policy_mapping_fn` from config dict. Helpful
            # is users would like to specify custom callable classes in yaml files.
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
                "True|False (trainable or not?)."
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

        if policy_states_are_swappable is not None:
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
        min_time_s_per_iteration: Optional[int] = NotProvided,
        min_train_timesteps_per_iteration: Optional[int] = NotProvided,
        min_sample_timesteps_per_iteration: Optional[int] = NotProvided,
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
            min_time_s_per_iteration: Minimum time to accumulate within a single
                `train()` call. This value does not affect learning,
                only the number of times `Algorithm.training_step()` is called by
                `Algorithm.train()`. If - after one such step attempt, the time taken
                has not reached `min_time_s_per_iteration`, will perform n more
                `training_step()` calls until the minimum time has been
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
        log_level: Optional[str] = DEPRECATED_VALUE,
        log_sys_usage: Optional[bool] = NotProvided,
        fake_sampler: Optional[bool] = NotProvided,
        seed: Optional[int] = NotProvided,
        worker_cls: Optional[Type[RolloutWorker]] = NotProvided,
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
                (this is also printed out once at startup at the INFO level). When using
                the `rllib train` command, you can also use the `-v` and `-vv` flags as
                shorthand for INFO and DEBUG.
            log_sys_usage: Log system resource metrics to results. This requires
                `psutil` to be installed for sys stats, and `gputil` for GPU metrics.
            fake_sampler: Use fake (infinite speed) sampler. For testing only.
            seed: This argument, in conjunction with worker_index, sets the random
                seed of each worker, so that identically configured trials will have
                identical results. This makes experiments reproducible.
            worker_cls: Use a custom RolloutWorker type for unit testing purpose.

        Returns:
            This updated AlgorithmConfig object.
        """
        if logger_creator is not NotProvided:
            self.logger_creator = logger_creator
        if logger_config is not NotProvided:
            self.logger_config = logger_config
        if log_level != DEPRECATED_VALUE:
            deprecation_warning(
                old="config.log_level",
                help=(
                    "RLlib no longer has a separate logging configuration from the rest"
                    " of Ray. Configure logging on the root logger; RLlib messages "
                    "will be propagated up the logger hierarchy to be handled there."
                ),
                error=False,
            )
        if log_sys_usage is not NotProvided:
            self.log_sys_usage = log_sys_usage
        if fake_sampler is not NotProvided:
            self.fake_sampler = fake_sampler
        if seed is not NotProvided:
            self.seed = seed
        if worker_cls is not NotProvided:
            self.worker_cls = worker_cls

        return self

    @ExperimentalAPI
    def rl_module(
        self,
        *,
        rl_module_class: Optional[Type["RLModule"]] = NotProvided,
        _enable_rl_module_api: Optional[bool] = NotProvided,
    ) -> "AlgorithmConfig":
        """Sets the config's RLModule settings.

        Args:
            rl_module_class: The RLModule class to use for this config.
            _enable_rl_module_api: Whether to enable the RLModule API for this config.
                By default if you call `config.rl_module(rl_module=MyRLModule)`, the
                RLModule API will be enabled. If you want to disable it, you can call
                `config.rl_module(_enable_rl_module_api=False)`.

        Returns:
            This updated AlgorithmConfig object.
        """
        if rl_module_class is not NotProvided:
            self.rl_module_class = rl_module_class

        if self._enable_rl_module_api is not NotProvided:
            self._enable_rl_module_api = _enable_rl_module_api
        else:
            # throw a warning if the user has used this API but not enabled it.
            logger.warning(
                "You have called `config.rl_module(...)` but "
                "have not enabled the RLModule API. To enable it, call "
                "`config.rl_module(_enable_rl_module_api=True)`."
            )

        return self

    def experimental(
        self,
        *,
        _tf_policy_handles_more_than_one_loss: Optional[bool] = NotProvided,
        _disable_preprocessor_api: Optional[bool] = NotProvided,
        _disable_action_flattening: Optional[bool] = NotProvided,
        _disable_execution_plan_api: Optional[bool] = NotProvided,
    ) -> "AlgorithmConfig":
        """Sets the config's experimental settings.

        Args:
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
            _disable_execution_plan_api: Experimental flag.
                If True, the execution plan API will not be used. Instead,
                a Algorithm's `training_iteration` method will be called as-is each
                training iteration.

        Returns:
            This updated AlgorithmConfig object.
        """
        if _tf_policy_handles_more_than_one_loss is not NotProvided:
            self._tf_policy_handles_more_than_one_loss = (
                _tf_policy_handles_more_than_one_loss
            )
        if _disable_preprocessor_api is not NotProvided:
            self._disable_preprocessor_api = _disable_preprocessor_api
        if _disable_action_flattening is not NotProvided:
            self._disable_action_flattening = _disable_action_flattening
        if _disable_execution_plan_api is not NotProvided:
            self._disable_execution_plan_api = _disable_execution_plan_api

        return self

    def get_rollout_fragment_length(self, worker_index: int = 0) -> int:
        """Automatically infers a proper rollout_fragment_length setting if "auto".

        Uses the simple formula:
        `rollout_fragment_length` = `train_batch_size` /
        (`num_envs_per_worker` * `num_rollout_workers`)

        If result is not a fraction AND `worker_index` is provided, will make
        those workers add another timestep, such that the overall batch size (across
        the workers) will add up to exactly the `train_batch_size`.

        Returns:
            The user-provided `rollout_fragment_length` or a computed one (if user
            value is "auto").
        """
        if self.rollout_fragment_length == "auto":
            # Example:
            # 2 workers, 2 envs per worker, 2000 train batch size:
            # -> 2000 / 4 -> 500
            # 4 workers, 3 envs per worker, 2500 train batch size:
            # -> 2500 / 12 -> 208.333 -> diff=4 (208 * 12 = 2496)
            # -> worker 1: 209, workers 2-4: 208
            rollout_fragment_length = self.train_batch_size / (
                self.num_envs_per_worker * (self.num_rollout_workers or 1)
            )
            if int(rollout_fragment_length) != rollout_fragment_length:
                diff = self.train_batch_size - int(
                    rollout_fragment_length
                ) * self.num_envs_per_worker * (self.num_rollout_workers or 1)
                if (worker_index * self.num_envs_per_worker) <= diff:
                    return int(rollout_fragment_length) + 1
            return int(rollout_fragment_length)
        else:
            return self.rollout_fragment_length

    def get_evaluation_config_object(
        self,
    ) -> Optional["AlgorithmConfig"]:
        """Creates a full AlgorithmConfig object from `self.evaluation_config`.

        Returns:
            A fully valid AlgorithmConfig object that can be used for the evaluation
            WorkerSet. If `self` is already an evaluation config object, return None.
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
                10
                if self.evaluation_duration == "auto"
                else int(
                    math.ceil(
                        self.evaluation_duration / (self.evaluation_num_workers or 1)
                    )
                )
            )

        return eval_config_obj

    def get_multi_agent_setup(
        self,
        *,
        policies: Optional[MultiAgentPolicyConfigDict] = None,
        env: Optional[EnvType] = None,
        spaces: Optional[Dict[PolicyID, Tuple[Space, Space]]] = None,
        default_policy_class: Optional[Type[Policy]] = None,
    ) -> Tuple[MultiAgentPolicyConfigDict, Callable[[PolicyID, SampleBatchType], bool]]:
        """Compiles complete multi-agent config (dict) from the information in `self`.

        Infers the observation- and action spaces, the policy classes, and the policy's
        configs. The returned `MultiAgentPolicyConfigDict` is fully unified and strictly
        maps PolicyIDs to complete PolicySpec objects (with all their fields not-None).

        Examples:
            >>> import numpy as np
            >>> from ray.rllib.algorithms.ppo import PPOConfig
            >>> config = (
            ...   PPOConfig()
            ...   .environment("CartPole-v1")
            ...   .framework("torch")
            ...   .multi_agent(policies={"pol1", "pol2"}, policies_to_train=["pol1"])
            ... )
            >>> policy_dict, is_policy_to_train = \  # doctest: +SKIP
            ...     config.get_multi_agent_setup()
            >>> is_policy_to_train("pol1") # doctest: +SKIP
            True
            >>> is_policy_to_train("pol2") # doctest: +SKIP
            False
            >>> print(policy_dict) # doctest: +SKIP
            {
              "pol1": PolicySpec(
                PPOTorchPolicyV2,  # infered from Algo's default policy class
                Box(-2.0, 2.0, (4,), np.float),  # infered from env
                Discrete(2),  # infered from env
                {},  # not provided -> empty dict
              ),
              "pol2": PolicySpec(
                PPOTorchPolicyV2,  # infered from Algo's default policy class
                Box(-2.0, 2.0, (4,), np.float),  # infered from env
                Discrete(2),  # infered from env
                {},  # not provided -> empty dict
              ),
            }

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
                RolloutWorker. If not provided, will try to infer from
                `env`. Otherwise from `self.observation_space` and
                `self.action_space`. If no information on spaces can be infered, will
                raise an error.
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

        # Check each defined policy ID and unify its spec.
        for pid, policy_spec in policies.copy().items():
            # Convert to PolicySpec if plain list/tuple.
            if not isinstance(policy_spec, PolicySpec):
                policies[pid] = policy_spec = PolicySpec(*policy_spec)

            # Infer policy classes for policies dict, if not provided (None).
            if policy_spec.policy_class is None and default_policy_class is not None:
                policies[pid].policy_class = default_policy_class

            # In case - somehow - an old gym Space made it to here, convert it
            # to the corresponding gymnasium space.
            if old_gym and isinstance(policy_spec.observation_space, old_gym.Space):
                policies[
                    pid
                ].observation_space = convert_old_gym_space_to_gymnasium_space(
                    policy_spec.observation_space
                )
            # Infer observation space.
            elif policy_spec.observation_space is None:
                if spaces is not None and pid in spaces:
                    obs_space = spaces[pid][0]
                elif env_obs_space is not None:
                    # Multi-agent case AND different agents have different spaces:
                    # Need to reverse map spaces (for the different agents) to certain
                    # policy IDs.
                    if (
                        isinstance(env, MultiAgentEnv)
                        and hasattr(env, "_obs_space_in_preferred_format")
                        and env._obs_space_in_preferred_format
                    ):
                        obs_space = None
                        mapping_fn = self.policy_mapping_fn
                        if mapping_fn:
                            for aid in env.get_agent_ids():
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

            # In case - somehow - an old gym Space made it to here, convert it
            # to the corresponding gymnasium space.
            if old_gym and isinstance(policy_spec.action_space, old_gym.Space):
                policies[pid].action_space = convert_old_gym_space_to_gymnasium_space(
                    policy_spec.action_space
                )
            # Infer action space.
            elif policy_spec.action_space is None:
                if spaces is not None and pid in spaces:
                    act_space = spaces[pid][1]
                elif env_act_space is not None:
                    # Multi-agent case AND different agents have different spaces:
                    # Need to reverse map spaces (for the different agents) to certain
                    # policy IDs.
                    if (
                        isinstance(env, MultiAgentEnv)
                        and hasattr(env, "_action_space_in_preferred_format")
                        and env._action_space_in_preferred_format
                    ):
                        act_space = None
                        mapping_fn = self.policy_mapping_fn
                        if mapping_fn:
                            for aid in env.get_agent_ids():
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

        # If container given, construct a simple default callable returning True
        # if the PolicyID is found in the list/set of IDs.
        is_policy_to_train = self.policies_to_train
        if self.policies_to_train is not None and not callable(self.policies_to_train):
            pols = set(self.policies_to_train)

            def is_policy_to_train(pid, batch=None):
                return pid in pols

        return policies, is_policy_to_train

    def validate_train_batch_size_vs_rollout_fragment_length(self) -> None:
        """Detects mismatches for `train_batch_size` vs `rollout_fragment_length`.

        Only applicable for algorithms, whose train_batch_size should be directly
        dependent on rollout_fragment_length (synchronous sampling, on-policy PG algos).

        If rollout_fragment_length != "auto", makes sure that the product of
        `rollout_fragment_length` x `num_rollout_workers` x `num_envs_per_worker`
        roughly (10%) matches the provided `train_batch_size`. Otherwise, errors with
        asking the user to set rollout_fragment_length to `auto` or to a matching
        value.

        Also, only checks this if `train_batch_size` > 0 (DDPPO sets this
        to -1 to auto-calculate the actual batch size later).

        Raises:
            ValueError: If there is a mismatch between user provided
            `rollout_fragment_length` and `train_batch_size`.
        """
        if (
            self.rollout_fragment_length != "auto"
            and not self.in_evaluation
            and self.train_batch_size > 0
        ):
            min_batch_size = (
                max(self.num_rollout_workers, 1)
                * self.num_envs_per_worker
                * self.rollout_fragment_length
            )
            batch_size = min_batch_size
            while batch_size < self.train_batch_size:
                batch_size += min_batch_size
            if (
                batch_size - self.train_batch_size > 0.1 * self.train_batch_size
                or batch_size - min_batch_size - self.train_batch_size
                > (0.1 * self.train_batch_size)
            ):
                suggested_rollout_fragment_length = self.train_batch_size // (
                    self.num_envs_per_worker * (self.num_rollout_workers or 1)
                )
                raise ValueError(
                    f"Your desired `train_batch_size` ({self.train_batch_size}) or a "
                    "value 10% off of that cannot be achieved with your other "
                    f"settings (num_rollout_workers={self.num_rollout_workers}; "
                    f"num_envs_per_worker={self.num_envs_per_worker}; "
                    f"rollout_fragment_length={self.rollout_fragment_length})! "
                    "Try setting `rollout_fragment_length` to 'auto' OR "
                    f"{suggested_rollout_fragment_length}."
                )

    def get_default_rl_module_class(self) -> Union[Type["RLModule"], str]:
        """Returns the RLModule class to use for this algorithm.

        Override this method in the sub-class to return the RLModule class type given
        the input framework.

        Returns:
            The RLModule class to use for this algorithm either as a class type or as
            a string (e.g. x.y.z).
        """
        raise NotImplementedError

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
        super().__setattr__(key, value)

    def __getitem__(self, item):
        """Shim method to still support accessing properties by key lookup.

        This way, an AlgorithmConfig object can still be used as if a dict, e.g.
        by Ray Tune.

        Examples:
            >>> from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
            >>> config = AlgorithmConfig()
            >>> print(config["lr"])
            ... 0.001
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
            key = "num_cpus_for_local_worker"
        elif key == "num_workers":
            key = "num_rollout_workers"

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
                f" with eager_tracing={self.framework_str}. For "
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

    @property
    def multiagent(self):
        """Shim method to help pretend we are a dict with 'multiagent' key."""
        return {
            "policies": self.policies,
            "policy_mapping_fn": self.policy_mapping_fn,
            "policies_to_train": self.policies_to_train,
            "policy_map_capacity": self.policy_map_capacity,
            "policy_map_cache": self.policy_map_cache,
            "count_steps_by": self.count_steps_by,
            "observation_fn": self.observation_fn,
        }

    @property
    @Deprecated(new="AlgorithmConfig.rollouts(num_rollout_workers=..)", error=False)
    def num_workers(self):
        """For backward-compatibility purposes only."""
        return self.num_rollout_workers

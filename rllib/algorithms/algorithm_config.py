import copy
import gym
from typing import (
    Any,
    Callable,
    Dict,
    Optional,
    Type,
    TYPE_CHECKING,
    Union,
)

from ray.rllib.algorithms.callbacks import DefaultCallbacks
from ray.rllib.env.env_context import EnvContext
from ray.rllib.evaluation.collectors.sample_collector import SampleCollector
from ray.rllib.evaluation.collectors.simple_list_collector import SimpleListCollector
from ray.rllib.models import MODEL_DEFAULTS
from ray.rllib.utils import deep_update, merge_dicts
from ray.rllib.utils.deprecation import DEPRECATED_VALUE, deprecation_warning
from ray.rllib.utils.typing import (
    EnvConfigDict,
    EnvType,
    PartialAlgorithmConfigDict,
    ResultDict,
    AlgorithmConfigDict,
)
from ray.tune.logger import Logger

if TYPE_CHECKING:
    from ray.rllib.algorithms.algorithm import Algorithm


class AlgorithmConfig:
    """A RLlib AlgorithmConfig builds an RLlib Algorithm from a given configuration.

    Example:
        >>> from ray.rllib.algorithms.callbacks import MemoryTrackingCallbacks
        >>> # Construct a generic config object, specifying values within different
        >>> # sub-categories, e.g. "training".
        >>> config = AlgorithmConfig().training(gamma=0.9, lr=0.01)
        ...              .environment(env="CartPole-v1")
        ...              .resources(num_gpus=0)
        ...              .rollouts(num_rollout_workers=4)
        ...              .callbacks(MemoryTrackingCallbacks)
        >>> # A config object can be used to construct the respective Trainer.
        >>> rllib_trainer = config.build()

    Example:
        >>> from ray import tune
        >>> # In combination with a tune.grid_search:
        >>> config = AlgorithmConfig()
        >>> config.training(lr=tune.grid_search([0.01, 0.001]))
        >>> # Use `to_dict()` method to get the legacy plain python config dict
        >>> # for usage with `tune.run()`.
        >>> tune.run("[registered trainer class]", config=config.to_dict())
    """

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

        # `self.rollouts()`
        self.num_workers = 2
        self.num_envs_per_worker = 1
        self.sample_collector = SimpleListCollector
        self.create_env_on_local_worker = False
        self.sample_async = False
        self.rollout_fragment_length = 200
        self.batch_mode = "truncate_episodes"
        self.remote_worker_envs = False
        self.remote_env_batch_wait_ms = 0
        self.validate_workers_after_construction = True
        self.ignore_worker_failures = False
        self.recreate_failed_workers = False
        self.restart_failed_sub_environments = False
        self.num_consecutive_worker_failures_tolerance = 100
        self.horizon = None
        self.soft_horizon = False
        self.no_done_at_end = False
        self.preprocessor_pref = "deepmind"
        self.observation_filter = "NoFilter"
        self.synchronize_filters = True
        self.compress_observations = False

        # `self.training()`
        self.gamma = 0.99
        self.lr = 0.001
        self.train_batch_size = 32
        self.model = copy.deepcopy(MODEL_DEFAULTS)
        self.optimizer = {}

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
        self.policies = {}
        self.policy_map_capacity = 100
        self.policy_map_cache = None
        self.policy_mapping_fn = None
        self.policies_to_train = None
        self.observation_fn = None
        self.replay_mode = "independent"
        self.count_steps_by = "env_steps"

        # `self.offline_data()`
        self.input_ = "sampler"
        self.input_config = {}
        self.actions_in_input_normalized = False
        self.off_policy_estimation_methods = {}
        self.postprocess_inputs = False
        self.shuffle_buffer_size = 0
        self.output = None
        self.output_config = {}
        self.output_compress_columns = ["obs", "new_obs"]
        self.output_max_file_size = 64 * 1024 * 1024

        # `self.evaluation()`
        self.evaluation_interval = None
        self.evaluation_duration = 10
        self.evaluation_duration_unit = "episodes"
        self.evaluation_parallel_to_training = False
        self.evaluation_config = {}
        self.evaluation_num_workers = 0
        self.custom_evaluation_function = None
        self.always_attach_evaluation_results = False
        # TODO: Set this flag still in the config or - much better - in the
        #  RolloutWorker as a property.
        self.in_evaluation = False
        self.sync_filters_on_rollout_workers_timeout_s = 60.0

        # `self.reporting()`
        self.keep_per_episode_custom_metrics = False
        self.metrics_episode_collection_timeout_s = 180
        self.metrics_num_episodes_for_smoothing = 100
        self.min_time_s_per_iteration = None
        self.min_train_timesteps_per_iteration = 0
        self.min_sample_timesteps_per_iteration = 0

        # `self.debugging()`
        self.logger_creator = None
        self.logger_config = None
        self.log_level = "WARN"
        self.log_sys_usage = True
        self.fake_sampler = False
        self.seed = None

        # `self.experimental()`
        self._tf_policy_handles_more_than_one_loss = False
        self._disable_preprocessor_api = False
        self._disable_action_flattening = False
        self._disable_execution_plan_api = True

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
        # The following values have moved because of the new ReplayBuffer API
        self.buffer_size = DEPRECATED_VALUE
        self.prioritized_replay = DEPRECATED_VALUE
        self.learning_starts = DEPRECATED_VALUE
        self.replay_batch_size = DEPRECATED_VALUE
        # -1 = DEPRECATED_VALUE is a valid value for replay_sequence_length
        self.replay_sequence_length = None
        self.prioritized_replay_alpha = DEPRECATED_VALUE
        self.prioritized_replay_beta = DEPRECATED_VALUE
        self.prioritized_replay_eps = DEPRECATED_VALUE
        self.min_time_s_per_reporting = DEPRECATED_VALUE
        self.min_train_timesteps_per_reporting = DEPRECATED_VALUE
        self.min_sample_timesteps_per_reporting = DEPRECATED_VALUE
        self.input_evaluation = DEPRECATED_VALUE

    def to_dict(self) -> AlgorithmConfigDict:
        """Converts all settings into a legacy config dict for backward compatibility.

        Returns:
            A complete AlgorithmConfigDict, usable in backward-compatible Tune/RLlib
            use cases, e.g. w/ `tune.run()`.
        """
        config = copy.deepcopy(vars(self))
        config.pop("algo_class")

        # Worst naming convention ever: NEVER EVER use reserved key-words...
        if "lambda_" in config:
            assert hasattr(self, "lambda_")
            config["lambda"] = getattr(self, "lambda_")
            config.pop("lambda_")
        if "input_" in config:
            assert hasattr(self, "input_")
            config["input"] = getattr(self, "input_")
            config.pop("input_")

        # Setup legacy multiagent sub-dict:
        config["multiagent"] = {}
        for k in [
            "policies",
            "policy_map_capacity",
            "policy_map_cache",
            "policy_mapping_fn",
            "policies_to_train",
            "observation_fn",
            "replay_mode",
            "count_steps_by",
        ]:
            config["multiagent"][k] = config.pop(k)

        # Switch out deprecated vs new config keys.
        config["callbacks"] = config.pop("callbacks_class", DefaultCallbacks)
        config["create_env_on_driver"] = config.pop("create_env_on_local_worker", 1)
        config["custom_eval_function"] = config.pop("custom_evaluation_function", None)
        config["framework"] = config.pop("framework_str", None)
        config["num_cpus_for_driver"] = config.pop("num_cpus_for_local_worker", 1)

        return config

    def build(
        self,
        env: Optional[Union[str, EnvType]] = None,
        logger_creator: Optional[Callable[[], Logger]] = None,
    ) -> "Algorithm":
        """Builds an Algorithm from the AlgorithmConfig.

        Args:
            env: Name of the environment to use (e.g. a gym-registered str),
                a full class path (e.g.
                "ray.rllib.examples.env.random_env.RandomEnv"), or an Env
                class directly. Note that this arg can also be specified via
                the "env" key in `config`.
            logger_creator: Callable that creates a ray.tune.Logger
                object. If unspecified, a default logger is created.

        Returns:
            A ray.rllib.algorithms.algorithm.Algorithm object.
        """
        if env is not None:
            self.env = env
            if self.evaluation_config is not None:
                self.evaluation_config["env"] = env
        if logger_creator is not None:
            self.logger_creator = logger_creator

        return self.algo_class(
            config=self.to_dict(),
            env=self.env,
            logger_creator=self.logger_creator,
        )

    def python_environment(
        self,
        *,
        extra_python_environs_for_driver: Optional[dict] = None,
        extra_python_environs_for_worker: Optional[dict] = None,
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
        if extra_python_environs_for_driver is not None:
            self.extra_python_environs_for_driver = extra_python_environs_for_driver
        if extra_python_environs_for_worker is not None:
            self.extra_python_environs_for_worker = extra_python_environs_for_worker
        return self

    def resources(
        self,
        *,
        num_gpus: Optional[Union[float, int]] = None,
        _fake_gpus: Optional[bool] = None,
        num_cpus_per_worker: Optional[int] = None,
        num_gpus_per_worker: Optional[Union[float, int]] = None,
        num_cpus_for_local_worker: Optional[int] = None,
        custom_resources_per_worker: Optional[dict] = None,
        placement_strategy: Optional[str] = None,
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
        if num_gpus is not None:
            self.num_gpus = num_gpus
        if _fake_gpus is not None:
            self._fake_gpus = _fake_gpus
        if num_cpus_per_worker is not None:
            self.num_cpus_per_worker = num_cpus_per_worker
        if num_gpus_per_worker is not None:
            self.num_gpus_per_worker = num_gpus_per_worker
        if num_cpus_for_local_worker is not None:
            self.num_cpus_for_local_worker = num_cpus_for_local_worker
        if custom_resources_per_worker is not None:
            self.custom_resources_per_worker = custom_resources_per_worker
        if placement_strategy is not None:
            self.placement_strategy = placement_strategy

        return self

    def framework(
        self,
        framework: Optional[str] = None,
        *,
        eager_tracing: Optional[bool] = None,
        eager_max_retraces: Optional[int] = None,
        tf_session_args: Optional[Dict[str, Any]] = None,
        local_tf_session_args: Optional[Dict[str, Any]] = None,
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
                Only necessary for framework=[tf2|tfe].
                Set to None to ignore the re-trace count and never throw an error.
            tf_session_args: Configures TF for single-process operation by default.
            local_tf_session_args: Override the following tf session args on the local
                worker

        Returns:
            This updated AlgorithmConfig object.
        """
        if framework is not None:
            self.framework_str = framework
        if eager_tracing is not None:
            self.eager_tracing = eager_tracing
        if eager_max_retraces is not None:
            self.eager_max_retraces = eager_max_retraces
        if tf_session_args is not None:
            self.tf_session_args = tf_session_args
        if local_tf_session_args is not None:
            self.local_tf_session_args = local_tf_session_args

        return self

    def environment(
        self,
        *,
        env: Optional[Union[str, EnvType]] = None,
        env_config: Optional[EnvConfigDict] = None,
        observation_space: Optional[gym.spaces.Space] = None,
        action_space: Optional[gym.spaces.Space] = None,
        env_task_fn: Optional[Callable[[ResultDict, EnvType, EnvContext], Any]] = None,
        render_env: Optional[bool] = None,
        clip_rewards: Optional[Union[bool, float]] = None,
        normalize_actions: Optional[bool] = None,
        clip_actions: Optional[bool] = None,
        disable_env_checking: Optional[bool] = None,
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
                object (which is a dict plus the properties: num_workers, worker_index,
                vector_index, and remote).
            observation_space: The observation space for the Policies of this Algorithm.
            action_space: The action space for the Policies of this Algorithm.
            env_task_fn: A callable taking the last train results, the base env and the
                env context as args and returning a new task to set the env to.
                The env must be a `TaskSettableEnv` sub-class for this to work.
                See `examples/curriculum_learning.py` for an example.
            render_env: If True, try to render the environment on the local worker or on
                worker 1 (if num_workers > 0). For vectorized envs, this usually means
                that only the first sub-environment will be rendered.
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

        Returns:
            This updated AlgorithmConfig object.
        """
        if env is not None:
            self.env = env
        if env_config is not None:
            self.env_config = env_config
        if observation_space is not None:
            self.observation_space = observation_space
        if action_space is not None:
            self.action_space = action_space
        if env_task_fn is not None:
            self.env_task_fn = env_task_fn
        if render_env is not None:
            self.render_env = render_env
        if clip_rewards is not None:
            self.clip_rewards = clip_rewards
        if normalize_actions is not None:
            self.normalize_actions = normalize_actions
        if clip_actions is not None:
            self.clip_actions = clip_actions
        if disable_env_checking is not None:
            self.disable_env_checking = disable_env_checking

        return self

    def rollouts(
        self,
        *,
        num_rollout_workers: Optional[int] = None,
        num_envs_per_worker: Optional[int] = None,
        create_env_on_local_worker: Optional[bool] = None,
        sample_collector: Optional[Type[SampleCollector]] = None,
        sample_async: Optional[bool] = None,
        rollout_fragment_length: Optional[int] = None,
        batch_mode: Optional[str] = None,
        remote_worker_envs: Optional[bool] = None,
        remote_env_batch_wait_ms: Optional[float] = None,
        validate_workers_after_construction: Optional[bool] = None,
        ignore_worker_failures: Optional[bool] = None,
        recreate_failed_workers: Optional[bool] = None,
        restart_failed_sub_environments: Optional[bool] = None,
        num_consecutive_worker_failures_tolerance: Optional[int] = None,
        horizon: Optional[int] = None,
        soft_horizon: Optional[bool] = None,
        no_done_at_end: Optional[bool] = None,
        preprocessor_pref: Optional[str] = None,
        observation_filter: Optional[str] = None,
        synchronize_filter: Optional[bool] = None,
        compress_observations: Optional[bool] = None,
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
            create_env_on_local_worker: When `num_workers` > 0, the driver
                (local_worker; worker-idx=0) does not need an environment. This is
                because it doesn't have to sample (done by remote_workers;
                worker_indices > 0) nor evaluate (done by evaluation workers;
                see below).
            sample_async: Use a background thread for sampling (slightly off-policy,
                usually not advisable to turn on unless your env specifically requires
                it).
            rollout_fragment_length: Divide episodes into fragments of this many steps
                each during rollouts. Sample batches of this size are collected from
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
            batch_mode: How to build per-Sampler (RolloutWorker) batches, which are then
                usually concat'd to form the train batch. Note that "steps" below can
                mean different things (either env- or agent-steps) and depends on the
                `count_steps_by` (multiagent) setting below.
                "truncate_episodes": Each produced batch (when calling
                RolloutWorker.sample()) will contain exactly `rollout_fragment_length`
                steps. This mode guarantees evenly sized batches, but increases
                variance as the future return must now be estimated at truncation
                boundaries.
                "complete_episodes": Each unroll happens exactly over one episode, from
                beginning to end. Data collection will not stop unless the episode
                terminates or a configured horizon (hard or soft) is hit.
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
            horizon: Number of steps after which the episode is forced to terminate.
                Defaults to `env.spec.max_episode_steps` (if present) for Gym envs.
            soft_horizon: Calculate rewards but don't reset the environment when the
                horizon is hit. This allows value estimation and RNN state to span
                across logical episodes denoted by horizon. This only has an effect
                if horizon != inf.
            no_done_at_end: Don't set 'done' at the end of the episode.
                In combination with `soft_horizon`, this works as follows:
                - no_done_at_end=False soft_horizon=False:
                Reset env and add `done=True` at end of each episode.
                - no_done_at_end=True soft_horizon=False:
                Reset env, but do NOT add `done=True` at end of the episode.
                - no_done_at_end=False soft_horizon=True:
                Do NOT reset env at horizon, but add `done=True` at the horizon
                (pretending the episode has terminated).
                - no_done_at_end=True soft_horizon=True:
                Do NOT reset env at horizon and do NOT add `done=True` at the horizon.
            preprocessor_pref: Whether to use "rllib" or "deepmind" preprocessors by
                default. Set to None for using no preprocessor. In this case, the
                model will have to handle possibly complex observations from the
                environment.
            observation_filter: Element-wise observation filter, either "NoFilter"
                or "MeanStdFilter".
            synchronize_filter: Whether to synchronize the statistics of remote filters.
            compress_observations: Whether to LZ4 compress individual observations
                in the SampleBatches collected during rollouts.

        Returns:
            This updated AlgorithmConfig object.
        """
        if num_rollout_workers is not None:
            self.num_workers = num_rollout_workers
        if num_envs_per_worker is not None:
            self.num_envs_per_worker = num_envs_per_worker
        if sample_collector is not None:
            self.sample_collector = sample_collector
        if create_env_on_local_worker is not None:
            self.create_env_on_local_worker = create_env_on_local_worker
        if sample_async is not None:
            self.sample_async = sample_async
        if rollout_fragment_length is not None:
            self.rollout_fragment_length = rollout_fragment_length
        if batch_mode is not None:
            self.batch_mode = batch_mode
        if remote_worker_envs is not None:
            self.remote_worker_envs = remote_worker_envs
        if remote_env_batch_wait_ms is not None:
            self.remote_env_batch_wait_ms = remote_env_batch_wait_ms
        if validate_workers_after_construction is not None:
            self.validate_workers_after_construction = (
                validate_workers_after_construction
            )
        if ignore_worker_failures is not None:
            self.ignore_worker_failures = ignore_worker_failures
        if recreate_failed_workers is not None:
            self.recreate_failed_workers = recreate_failed_workers
        if restart_failed_sub_environments is not None:
            self.restart_failed_sub_environments = restart_failed_sub_environments
        if num_consecutive_worker_failures_tolerance is not None:
            self.num_consecutive_worker_failures_tolerance = (
                num_consecutive_worker_failures_tolerance
            )
        if horizon is not None:
            self.horizon = horizon
        if soft_horizon is not None:
            self.soft_horizon = soft_horizon
        if no_done_at_end is not None:
            self.no_done_at_end = no_done_at_end
        if preprocessor_pref is not None:
            self.preprocessor_pref = preprocessor_pref
        if observation_filter is not None:
            self.observation_filter = observation_filter
        if synchronize_filter is not None:
            self.synchronize_filters = synchronize_filter
        if compress_observations is not None:
            self.compress_observations = compress_observations

        return self

    def training(
        self,
        gamma: Optional[float] = None,
        lr: Optional[float] = None,
        train_batch_size: Optional[int] = None,
        model: Optional[dict] = None,
        optimizer: Optional[dict] = None,
    ) -> "AlgorithmConfig":
        """Sets the training related configuration.

        Args:
            gamma: Float specifying the discount factor of the Markov Decision process.
            lr: The default learning rate.
            train_batch_size: Training batch size, if applicable.
            model: Arguments passed into the policy model. See models/catalog.py for a
                full list of the available model options.
            optimizer: Arguments to pass to the policy optimizer.

        Returns:
            This updated AlgorithmConfig object.
        """
        if gamma is not None:
            self.gamma = gamma
        if lr is not None:
            self.lr = lr
        if train_batch_size is not None:
            self.train_batch_size = train_batch_size
        if model is not None:
            self.model = model
        if optimizer is not None:
            self.optimizer = merge_dicts(self.optimizer, optimizer)

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
        self.callbacks_class = callbacks_class

        return self

    def exploration(
        self,
        *,
        explore: Optional[bool] = None,
        exploration_config: Optional[dict] = None,
    ) -> "AlgorithmConfig":
        """Sets the config's exploration settings.

        Args:
            explore: Default exploration behavior, iff `explore`=None is passed into
                compute_action(s). Set to False for no exploration behavior (e.g.,
                for evaluation).
            exploration_config: A dict specifying the Exploration object's config.

        Returns:
            This updated AlgorithmConfig object.
        """
        if explore is not None:
            self.explore = explore
        if exploration_config is not None:
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
        evaluation_interval: Optional[int] = None,
        evaluation_duration: Optional[int] = None,
        evaluation_duration_unit: Optional[str] = None,
        evaluation_parallel_to_training: Optional[bool] = None,
        evaluation_config: Optional[
            Union["AlgorithmConfig", PartialAlgorithmConfigDict]
        ] = None,
        evaluation_num_workers: Optional[int] = None,
        custom_evaluation_function: Optional[Callable] = None,
        always_attach_evaluation_results: Optional[bool] = None,
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

        Returns:
            This updated AlgorithmConfig object.
        """
        if evaluation_interval is not None:
            self.evaluation_interval = evaluation_interval
        if evaluation_duration is not None:
            self.evaluation_duration = evaluation_duration
        if evaluation_duration_unit is not None:
            self.evaluation_duration_unit = evaluation_duration_unit
        if evaluation_parallel_to_training is not None:
            self.evaluation_parallel_to_training = evaluation_parallel_to_training
        if evaluation_config is not None:
            # Convert another AlgorithmConfig into dict.
            if isinstance(evaluation_config, AlgorithmConfig):
                self.evaluation_config = evaluation_config.to_dict()
            else:
                self.evaluation_config = evaluation_config
        if evaluation_num_workers is not None:
            self.evaluation_num_workers = evaluation_num_workers
        if custom_evaluation_function is not None:
            self.custom_evaluation_function = custom_evaluation_function
        if always_attach_evaluation_results:
            self.always_attach_evaluation_results = always_attach_evaluation_results

        return self

    def offline_data(
        self,
        *,
        input_=None,
        input_config=None,
        actions_in_input_normalized=None,
        input_evaluation=None,
        off_policy_estimation_methods=None,
        postprocess_inputs=None,
        shuffle_buffer_size=None,
        output=None,
        output_config=None,
        output_compress_columns=None,
        output_max_file_size=None,
    ) -> "AlgorithmConfig":
        """Sets the config's offline data settings.

        TODO(jungong, sven): we can potentially unify all input types
          under input and input_config keys. E.g.
          input: sample
          input_config {
            env: Cartpole-v0
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
            input_config: Arguments accessible from the IOContext for configuring custom
                input.
            actions_in_input_normalized: True, if the actions in a given offline "input"
                are already normalized (between -1.0 and 1.0). This is usually the case
                when the offline file has been generated by another RLlib algorithm
                (e.g. PPO or SAC), while "normalize_actions" was set to True.
            input_evaluation: DEPRECATED: Use `off_policy_estimation_methods` instead!
            off_policy_estimation_methods: Specify how to evaluate the current policy,
                along with any optional config parameters.
                This only has an effect when reading offline experiences
                ("input" is not "sampler").
                Available keys:
                - {ope_method_name: {"type": ope_type, ...}} where `ope_method_name`
                is a user-defined string to save the OPE results under, and
                `ope_type` can be:
                    - "simulation": Run the environment in the background, but use
                    this data for evaluation only and not for learning.
                    - Any subclass of OffPolicyEstimator, e.g.
                    ray.rllib.offline.estimators.is::ImportanceSampling
                    or your own custom subclass.
                You can also add additional config arguments to be passed to the
                OffPolicyEstimator in the dict, e.g.
                {"qreg_dr": {"type": DoublyRobust, "q_model_type": "qreg", "k": 5}}
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
            output_max_file_size: Max output file size before rolling over to a
                new file.

        Returns:
            This updated AlgorithmConfig object.
        """
        if input_ is not None:
            self.input_ = input_
        if input_config is not None:
            self.input_config = input_config
        if actions_in_input_normalized is not None:
            self.actions_in_input_normalized = actions_in_input_normalized
        if input_evaluation is not None:
            deprecation_warning(
                old="offline_data(input_evaluation={})".format(input_evaluation),
                new="offline_data(off_policy_estimation_methods={})".format(
                    input_evaluation
                ),
                error=True,
            )
        if isinstance(off_policy_estimation_methods, list) or isinstance(
            off_policy_estimation_methods, tuple
        ):
            ope_dict = {
                str(ope): {"type": ope} for ope in off_policy_estimation_methods
            }
            deprecation_warning(
                old="offline_data(off_policy_estimation_methods={}".format(
                    off_policy_estimation_methods
                ),
                new="offline_data(off_policy_estimation_methods={}".format(
                    ope_dict,
                ),
                error=False,
            )
            off_policy_estimation_methods = ope_dict
        if off_policy_estimation_methods is not None:
            self.off_policy_estimation_methods = off_policy_estimation_methods

        if postprocess_inputs is not None:
            self.postprocess_inputs = postprocess_inputs
        if shuffle_buffer_size is not None:
            self.shuffle_buffer_size = shuffle_buffer_size
        if output is not None:
            self.output = output
        if output_config is not None:
            self.output_config = output_config
        if output_compress_columns is not None:
            self.output_compress_columns = output_compress_columns
        if output_max_file_size is not None:
            self.output_max_file_size = output_max_file_size

        return self

    def multi_agent(
        self,
        *,
        policies=None,
        policy_map_capacity=None,
        policy_map_cache=None,
        policy_mapping_fn=None,
        policies_to_train=None,
        observation_fn=None,
        replay_mode=None,
        count_steps_by=None,
    ) -> "AlgorithmConfig":
        """Sets the config's multi-agent settings.

        Args:
            policies: Map of type MultiAgentPolicyConfigDict from policy ids to tuples
                of (policy_cls, obs_space, act_space, config). This defines the
                observation and action spaces of the policies and any extra config.
            policy_map_capacity: Keep this many policies in the "policy_map" (before
                writing least-recently used ones to disk/S3).
            policy_map_cache: Where to store overflowing (least-recently used) policies?
                Could be a directory (str) or an S3 location. None for using the
                default output dir.
            policy_mapping_fn: Function mapping agent ids to policy ids.
            policies_to_train: Determines those policies that should be updated.
                Options are:
                - None, for all policies.
                - An iterable of PolicyIDs that should be updated.
                - A callable, taking a PolicyID and a SampleBatch or MultiAgentBatch
                and returning a bool (indicating whether the given policy is trainable
                or not, given the particular batch). This allows you to have a policy
                trained only on certain data (e.g. when playing against a certain
                opponent).
            observation_fn: Optional function that can be used to enhance the local
                agent observations to include more state. See
                rllib/evaluation/observation_function.py for more info.
            replay_mode: When replay_mode=lockstep, RLlib will replay all the agent
                transitions at a particular timestep together in a batch. This allows
                the policy to implement differentiable shared computations between
                agents it controls at that timestep. When replay_mode=independent,
                transitions are replayed independently per policy.
            count_steps_by: Which metric to use as the "batch size" when building a
                MultiAgentBatch. The two supported values are:
                "env_steps": Count each time the env is "stepped" (no matter how many
                multi-agent actions are passed/how many multi-agent observations
                have been returned in the previous step).
                "agent_steps": Count each individual agent step as one step.

        Returns:
            This updated AlgorithmConfig object.
        """
        if policies is not None:
            self.policies = policies
        if policy_map_capacity is not None:
            self.policy_map_capacity = policy_map_capacity
        if policy_map_cache is not None:
            self.policy_map_cache = policy_map_cache
        if policy_mapping_fn is not None:
            self.policy_mapping_fn = policy_mapping_fn
        if policies_to_train is not None:
            self.policies_to_train = policies_to_train
        if observation_fn is not None:
            self.observation_fn = observation_fn
        if replay_mode is not None:
            self.replay_mode = replay_mode
        if count_steps_by is not None:
            self.count_steps_by = count_steps_by

        return self

    def reporting(
        self,
        *,
        keep_per_episode_custom_metrics: Optional[bool] = None,
        metrics_episode_collection_timeout_s: Optional[int] = None,
        metrics_num_episodes_for_smoothing: Optional[int] = None,
        min_time_s_per_iteration: Optional[int] = None,
        min_train_timesteps_per_iteration: Optional[int] = None,
        min_sample_timesteps_per_iteration: Optional[int] = None,
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
        if keep_per_episode_custom_metrics is not None:
            self.keep_per_episode_custom_metrics = keep_per_episode_custom_metrics
        if metrics_episode_collection_timeout_s is not None:
            self.metrics_episode_collection_timeout_s = (
                metrics_episode_collection_timeout_s
            )
        if metrics_num_episodes_for_smoothing is not None:
            self.metrics_num_episodes_for_smoothing = metrics_num_episodes_for_smoothing
        if min_time_s_per_iteration is not None:
            self.min_time_s_per_iteration = min_time_s_per_iteration
        if min_train_timesteps_per_iteration is not None:
            self.min_train_timesteps_per_iteration = min_train_timesteps_per_iteration
        if min_sample_timesteps_per_iteration is not None:
            self.min_sample_timesteps_per_iteration = min_sample_timesteps_per_iteration

        return self

    def debugging(
        self,
        *,
        logger_creator: Optional[Callable[[], Logger]] = None,
        logger_config: Optional[dict] = None,
        log_level: Optional[str] = None,
        log_sys_usage: Optional[bool] = None,
        fake_sampler: Optional[bool] = None,
        seed: Optional[int] = None,
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

        Returns:
            This updated AlgorithmConfig object.
        """
        if logger_creator is not None:
            self.logger_creator = logger_creator
        if logger_config is not None:
            self.logger_config = logger_config
        if log_level is not None:
            self.log_level = log_level
        if log_sys_usage is not None:
            self.log_sys_usage = log_sys_usage
        if fake_sampler is not None:
            self.fake_sampler = fake_sampler
        if seed is not None:
            self.seed = seed

        return self

    def experimental(
        self,
        *,
        _tf_policy_handles_more_than_one_loss=None,
        _disable_preprocessor_api=None,
        _disable_action_flattening=None,
        _disable_execution_plan_api=None,
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
        if _tf_policy_handles_more_than_one_loss is not None:
            self._tf_policy_handles_more_than_one_loss = (
                _tf_policy_handles_more_than_one_loss
            )
        if _disable_preprocessor_api is not None:
            self._disable_preprocessor_api = _disable_preprocessor_api
        if _disable_action_flattening is not None:
            self._disable_action_flattening = _disable_action_flattening
        if _disable_execution_plan_api is not None:
            self._disable_execution_plan_api = _disable_execution_plan_api

        return self

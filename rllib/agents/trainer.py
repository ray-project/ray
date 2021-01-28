from datetime import datetime
import numpy as np
import copy
import logging
import math
import os
import pickle
import time
import tempfile
from typing import Callable, Dict, List, Optional, Type, Union

import ray
from ray.exceptions import RayError
from ray.rllib.agents.callbacks import DefaultCallbacks
from ray.rllib.env.normalize_actions import NormalizeActionWrapper
from ray.rllib.env.env_context import EnvContext
from ray.rllib.evaluation.collectors.simple_list_collector import \
    SimpleListCollector
from ray.rllib.evaluation.rollout_worker import RolloutWorker
from ray.rllib.models import MODEL_DEFAULTS
from ray.rllib.policy import Policy
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID
from ray.rllib.evaluation.metrics import collect_metrics
from ray.rllib.evaluation.worker_set import WorkerSet
from ray.rllib.utils import FilterManager, deep_update, merge_dicts
from ray.rllib.utils.spaces import space_utils
from ray.rllib.utils.framework import try_import_tf, TensorStructType
from ray.rllib.utils.annotations import override, PublicAPI, DeveloperAPI
from ray.rllib.utils.deprecation import deprecation_warning, DEPRECATED_VALUE
from ray.rllib.utils.from_config import from_config
from ray.rllib.utils.typing import TrainerConfigDict, \
    PartialTrainerConfigDict, EnvInfoDict, ResultDict, EnvType, PolicyID
from ray.tune.registry import ENV_CREATOR, register_env, _global_registry
from ray.tune.trainable import Trainable
from ray.tune.trial import ExportFormat
from ray.tune.resources import Resources
from ray.tune.logger import Logger, UnifiedLogger
from ray.tune.result import DEFAULT_RESULTS_DIR

tf1, tf, tfv = try_import_tf()

logger = logging.getLogger(__name__)

# Max number of times to retry a worker failure. We shouldn't try too many
# times in a row since that would indicate a persistent cluster issue.
MAX_WORKER_FAILURE_RETRIES = 3

# yapf: disable
# __sphinx_doc_begin__
COMMON_CONFIG: TrainerConfigDict = {
    # === Settings for Rollout Worker processes ===
    # Number of rollout worker actors to create for parallel sampling. Setting
    # this to 0 will force rollouts to be done in the trainer actor.
    "num_workers": 2,
    # Number of environments to evaluate vectorwise per worker. This enables
    # model inference batching, which can improve performance for inference
    # bottlenecked workloads.
    "num_envs_per_worker": 1,
    # When `num_workers` > 0, the driver (local_worker; worker-idx=0) does not
    # need an environment. This is because it doesn't have to sample (done by
    # remote_workers; worker_indices > 0) nor evaluate (done by evaluation
    # workers; see below).
    "create_env_on_driver": False,
    # Divide episodes into fragments of this many steps each during rollouts.
    # Sample batches of this size are collected from rollout workers and
    # combined into a larger batch of `train_batch_size` for learning.
    #
    # For example, given rollout_fragment_length=100 and train_batch_size=1000:
    #   1. RLlib collects 10 fragments of 100 steps each from rollout workers.
    #   2. These fragments are concatenated and we perform an epoch of SGD.
    #
    # When using multiple envs per worker, the fragment size is multiplied by
    # `num_envs_per_worker`. This is since we are collecting steps from
    # multiple envs in parallel. For example, if num_envs_per_worker=5, then
    # rollout workers will return experiences in chunks of 5*100 = 500 steps.
    #
    # The dataflow here can vary per algorithm. For example, PPO further
    # divides the train batch into minibatches for multi-epoch SGD.
    "rollout_fragment_length": 200,
    # How to build per-Sampler (RolloutWorker) batches, which are then
    # usually concat'd to form the train batch. Note that "steps" below can
    # mean different things (either env- or agent-steps) and depends on the
    # `count_steps_by` (multiagent) setting below.
    # truncate_episodes: Each produced batch (when calling
    #   RolloutWorker.sample()) will contain exactly `rollout_fragment_length`
    #   steps. This mode guarantees evenly sized batches, but increases
    #   variance as the future return must now be estimated at truncation
    #   boundaries.
    # complete_episodes: Each unroll happens exactly over one episode, from
    #   beginning to end. Data collection will not stop unless the episode
    #   terminates or a configured horizon (hard or soft) is hit.
    "batch_mode": "truncate_episodes",

    # === Settings for the Trainer process ===
    # Number of GPUs to allocate to the trainer process. Note that not all
    # algorithms can take advantage of trainer GPUs. This can be fractional
    # (e.g., 0.3 GPUs).
    "num_gpus": 0,
    # Training batch size, if applicable. Should be >= rollout_fragment_length.
    # Samples batches will be concatenated together to a batch of this size,
    # which is then passed to SGD.
    "train_batch_size": 200,
    # Arguments to pass to the policy model. See models/catalog.py for a full
    # list of the available model options.
    "model": MODEL_DEFAULTS,
    # Arguments to pass to the policy optimizer. These vary by optimizer.
    "optimizer": {},

    # === Environment Settings ===
    # Discount factor of the MDP.
    "gamma": 0.99,
    # Number of steps after which the episode is forced to terminate. Defaults
    # to `env.spec.max_episode_steps` (if present) for Gym envs.
    "horizon": None,
    # Calculate rewards but don't reset the environment when the horizon is
    # hit. This allows value estimation and RNN state to span across logical
    # episodes denoted by horizon. This only has an effect if horizon != inf.
    "soft_horizon": False,
    # Don't set 'done' at the end of the episode. Note that you still need to
    # set this if soft_horizon=True, unless your env is actually running
    # forever without returning done=True.
    "no_done_at_end": False,
    # Arguments to pass to the env creator.
    "env_config": {},
    # Environment name can also be passed via config.
    "env": None,
    # Unsquash actions to the upper and lower bounds of env's action space
    "normalize_actions": False,
    # Whether to clip rewards during Policy's postprocessing.
    # None (default): Clip for Atari only (r=sign(r)).
    # True: r=sign(r): Fixed rewards -1.0, 1.0, or 0.0.
    # False: Never clip.
    # [float value]: Clip at -value and + value.
    # Tuple[value1, value2]: Clip at value1 and value2.
    "clip_rewards": None,
    # Whether to clip actions to the action space's low/high range spec.
    "clip_actions": True,
    # Whether to use "rllib" or "deepmind" preprocessors by default
    "preprocessor_pref": "deepmind",
    # The default learning rate.
    "lr": 0.0001,

    # === Debug Settings ===
    # Whether to write episode stats and videos to the agent log dir. This is
    # typically located in ~/ray_results.
    "monitor": False,
    # Set the ray.rllib.* log level for the agent process and its workers.
    # Should be one of DEBUG, INFO, WARN, or ERROR. The DEBUG level will also
    # periodically print out summaries of relevant internal dataflow (this is
    # also printed out once at startup at the INFO level). When using the
    # `rllib train` command, you can also use the `-v` and `-vv` flags as
    # shorthand for INFO and DEBUG.
    "log_level": "WARN",
    # Callbacks that will be run during various phases of training. See the
    # `DefaultCallbacks` class and `examples/custom_metrics_and_callbacks.py`
    # for more usage information.
    "callbacks": DefaultCallbacks,
    # Whether to attempt to continue training if a worker crashes. The number
    # of currently healthy workers is reported as the "num_healthy_workers"
    # metric.
    "ignore_worker_failures": False,
    # Log system resource metrics to results. This requires `psutil` to be
    # installed for sys stats, and `gputil` for GPU metrics.
    "log_sys_usage": True,
    # Use fake (infinite speed) sampler. For testing only.
    "fake_sampler": False,

    # === Deep Learning Framework Settings ===
    # tf: TensorFlow
    # tfe: TensorFlow eager
    # torch: PyTorch
    "framework": "tf",
    # Enable tracing in eager mode. This greatly improves performance, but
    # makes it slightly harder to debug since Python code won't be evaluated
    # after the initial eager pass. Only possible if framework=tfe.
    "eager_tracing": False,

    # === Exploration Settings ===
    # Default exploration behavior, iff `explore`=None is passed into
    # compute_action(s).
    # Set to False for no exploration behavior (e.g., for evaluation).
    "explore": True,
    # Provide a dict specifying the Exploration object's config.
    "exploration_config": {
        # The Exploration class to use. In the simplest case, this is the name
        # (str) of any class present in the `rllib.utils.exploration` package.
        # You can also provide the python class directly or the full location
        # of your class (e.g. "ray.rllib.utils.exploration.epsilon_greedy.
        # EpsilonGreedy").
        "type": "StochasticSampling",
        # Add constructor kwargs here (if any).
    },
    # === Evaluation Settings ===
    # Evaluate with every `evaluation_interval` training iterations.
    # The evaluation stats will be reported under the "evaluation" metric key.
    # Note that evaluation is currently not parallelized, and that for Ape-X
    # metrics are already only reported for the lowest epsilon workers.
    "evaluation_interval": None,
    # Number of episodes to run per evaluation period. If using multiple
    # evaluation workers, we will run at least this many episodes total.
    "evaluation_num_episodes": 10,
    # Internal flag that is set to True for evaluation workers.
    "in_evaluation": False,
    # Typical usage is to pass extra args to evaluation env creator
    # and to disable exploration by computing deterministic actions.
    # IMPORTANT NOTE: Policy gradient algorithms are able to find the optimal
    # policy, even if this is a stochastic one. Setting "explore=False" here
    # will result in the evaluation workers not using this optimal policy!
    "evaluation_config": {
        # Example: overriding env_config, exploration, etc:
        # "env_config": {...},
        # "explore": False
    },
    # Number of parallel workers to use for evaluation. Note that this is set
    # to zero by default, which means evaluation will be run in the trainer
    # process. If you increase this, it will increase the Ray resource usage
    # of the trainer since evaluation workers are created separately from
    # rollout workers.
    "evaluation_num_workers": 0,
    # Customize the evaluation method. This must be a function of signature
    # (trainer: Trainer, eval_workers: WorkerSet) -> metrics: dict. See the
    # Trainer._evaluate() method to see the default implementation. The
    # trainer guarantees all eval workers have the latest policy state before
    # this function is called.
    "custom_eval_function": None,

    # === Advanced Rollout Settings ===
    # Use a background thread for sampling (slightly off-policy, usually not
    # advisable to turn on unless your env specifically requires it).
    "sample_async": False,

    # Experimental flag to speed up sampling and use "trajectory views" as
    # generic ModelV2 `input_dicts` that can be requested by the model to
    # contain different information on the ongoing episode.
    "_use_trajectory_view_api": True,
    # The SampleCollector class to be used to collect and retrieve
    # environment-, model-, and sampler data. Override the SampleCollector base
    # class to implement your own collection/buffering/retrieval logic.
    "sample_collector": SimpleListCollector,

    # Element-wise observation filter, either "NoFilter" or "MeanStdFilter".
    "observation_filter": "NoFilter",
    # Whether to synchronize the statistics of remote filters.
    "synchronize_filters": True,
    # Configures TF for single-process operation by default.
    "tf_session_args": {
        # note: overridden by `local_tf_session_args`
        "intra_op_parallelism_threads": 2,
        "inter_op_parallelism_threads": 2,
        "gpu_options": {
            "allow_growth": True,
        },
        "log_device_placement": False,
        "device_count": {
            "CPU": 1
        },
        "allow_soft_placement": True,  # required by PPO multi-gpu
    },
    # Override the following tf session args on the local worker
    "local_tf_session_args": {
        # Allow a higher level of parallelism by default, but not unlimited
        # since that can cause crashes with many concurrent drivers.
        "intra_op_parallelism_threads": 8,
        "inter_op_parallelism_threads": 8,
    },
    # Whether to LZ4 compress individual observations
    "compress_observations": False,
    # Wait for metric batches for at most this many seconds. Those that
    # have not returned in time will be collected in the next train iteration.
    "collect_metrics_timeout": 180,
    # Smooth metrics over this many episodes.
    "metrics_smoothing_episodes": 100,
    # If using num_envs_per_worker > 1, whether to create those new envs in
    # remote processes instead of in the same worker. This adds overheads, but
    # can make sense if your envs can take much time to step / reset
    # (e.g., for StarCraft). Use this cautiously; overheads are significant.
    "remote_worker_envs": False,
    # Timeout that remote workers are waiting when polling environments.
    # 0 (continue when at least one env is ready) is a reasonable default,
    # but optimal value could be obtained by measuring your environment
    # step / reset and model inference perf.
    "remote_env_batch_wait_ms": 0,
    # Minimum time per train iteration (frequency of metrics reporting).
    "min_iter_time_s": 0,
    # Minimum env steps to optimize for per train call. This value does
    # not affect learning, only the length of train iterations.
    "timesteps_per_iteration": 0,
    # This argument, in conjunction with worker_index, sets the random seed of
    # each worker, so that identically configured trials will have identical
    # results. This makes experiments reproducible.
    "seed": None,
    # Any extra python env vars to set in the trainer process, e.g.,
    # {"OMP_NUM_THREADS": "16"}
    "extra_python_environs_for_driver": {},
    # The extra python environments need to set for worker processes.
    "extra_python_environs_for_worker": {},

    # === Advanced Resource Settings ===
    # Number of CPUs to allocate per worker.
    "num_cpus_per_worker": 1,
    # Number of GPUs to allocate per worker. This can be fractional. This is
    # usually needed only if your env itself requires a GPU (i.e., it is a
    # GPU-intensive video game), or model inference is unusually expensive.
    "num_gpus_per_worker": 0,
    # Any custom Ray resources to allocate per worker.
    "custom_resources_per_worker": {},
    # Number of CPUs to allocate for the trainer. Note: this only takes effect
    # when running in Tune. Otherwise, the trainer runs in the main program.
    "num_cpus_for_driver": 1,
    # Deprecated.
    "memory": 0,
    "object_store_memory": 0,
    "memory_per_worker": 0,
    "object_store_memory_per_worker": 0,

    # === Offline Datasets ===
    # Specify how to generate experiences:
    #  - "sampler": Generate experiences via online (env) simulation (default).
    #  - A local directory or file glob expression (e.g., "/tmp/*.json").
    #  - A list of individual file paths/URIs (e.g., ["/tmp/1.json",
    #    "s3://bucket/2.json"]).
    #  - A dict with string keys and sampling probabilities as values (e.g.,
    #    {"sampler": 0.4, "/tmp/*.json": 0.4, "s3://bucket/expert.json": 0.2}).
    #  - A callable that returns a ray.rllib.offline.InputReader.
    "input": "sampler",
    # Specify how to evaluate the current policy. This only has an effect when
    # reading offline experiences ("input" is not "sampler").
    # Available options:
    #  - "wis": the weighted step-wise importance sampling estimator.
    #  - "is": the step-wise importance sampling estimator.
    #  - "simulation": run the environment in the background, but use
    #    this data for evaluation only and not for learning.
    "input_evaluation": ["is", "wis"],
    # Whether to run postprocess_trajectory() on the trajectory fragments from
    # offline inputs. Note that postprocessing will be done using the *current*
    # policy, not the *behavior* policy, which is typically undesirable for
    # on-policy algorithms.
    "postprocess_inputs": False,
    # If positive, input batches will be shuffled via a sliding window buffer
    # of this number of batches. Use this if the input data is not in random
    # enough order. Input is delayed until the shuffle buffer is filled.
    "shuffle_buffer_size": 0,
    # Specify where experiences should be saved:
    #  - None: don't save any experiences
    #  - "logdir" to save to the agent log dir
    #  - a path/URI to save to a custom output directory (e.g., "s3://bucket/")
    #  - a function that returns a rllib.offline.OutputWriter
    "output": None,
    # What sample batch columns to LZ4 compress in the output data.
    "output_compress_columns": ["obs", "new_obs"],
    # Max output file size before rolling over to a new file.
    "output_max_file_size": 64 * 1024 * 1024,

    # === Settings for Multi-Agent Environments ===
    "multiagent": {
        # Map of type MultiAgentPolicyConfigDict from policy ids to tuples
        # of (policy_cls, obs_space, act_space, config). This defines the
        # observation and action spaces of the policies and any extra config.
        "policies": {},
        # Function mapping agent ids to policy ids.
        "policy_mapping_fn": None,
        # Optional list of policies to train, or None for all policies.
        "policies_to_train": None,
        # Optional function that can be used to enhance the local agent
        # observations to include more state.
        # See rllib/evaluation/observation_function.py for more info.
        "observation_fn": None,
        # When replay_mode=lockstep, RLlib will replay all the agent
        # transitions at a particular timestep together in a batch. This allows
        # the policy to implement differentiable shared computations between
        # agents it controls at that timestep. When replay_mode=independent,
        # transitions are replayed independently per policy.
        "replay_mode": "independent",
        # Which metric to use as the "batch size" when building a
        # MultiAgentBatch. The two supported values are:
        # env_steps: Count each time the env is "stepped" (no matter how many
        #   multi-agent actions are passed/how many multi-agent observations
        #   have been returned in the previous step).
        # agent_steps: Count each individual agent step as one step.
        "count_steps_by": "env_steps",
    },

    # === Logger ===
    # Define logger-specific configuration to be used inside Logger
    # Default value None allows overwriting with nested dicts
    "logger_config": None,

    # === Replay Settings ===
    # The number of contiguous environment steps to replay at once. This may
    # be set to greater than 1 to support recurrent models.
    "replay_sequence_length": 1,
}
# __sphinx_doc_end__
# yapf: enable


@DeveloperAPI
def with_common_config(
        extra_config: PartialTrainerConfigDict) -> TrainerConfigDict:
    """Returns the given config dict merged with common agent confs.

    Args:
        extra_config (PartialTrainerConfigDict): A user defined partial config
            which will get merged with COMMON_CONFIG and returned.

    Returns:
        TrainerConfigDict: The merged config dict resulting of COMMON_CONFIG
            plus `extra_config`.
    """
    return Trainer.merge_trainer_configs(
        COMMON_CONFIG, extra_config, _allow_unknown_configs=True)


@PublicAPI
class Trainer(Trainable):
    """A trainer coordinates the optimization of one or more RL policies.

    All RLlib trainers extend this base class, e.g., the A3CTrainer implements
    the A3C algorithm for single and multi-agent training.

    Trainer objects retain internal model state between calls to train(), so
    you should create a new trainer instance for each training session.

    Attributes:
        env_creator (func): Function that creates a new training env.
        config (obj): Algorithm-specific configuration data.
        logdir (str): Directory in which training outputs should be placed.
    """
    # Whether to allow unknown top-level config keys.
    _allow_unknown_configs = False

    # List of top-level keys with value=dict, for which new sub-keys are
    # allowed to be added to the value dict.
    _allow_unknown_subkeys = [
        "tf_session_args", "local_tf_session_args", "env_config", "model",
        "optimizer", "multiagent", "custom_resources_per_worker",
        "evaluation_config", "exploration_config",
        "extra_python_environs_for_driver", "extra_python_environs_for_worker"
    ]

    # List of top level keys with value=dict, for which we always override the
    # entire value (dict), iff the "type" key in that value dict changes.
    _override_all_subkeys_if_type_changes = ["exploration_config"]

    @PublicAPI
    def __init__(self,
                 config: TrainerConfigDict = None,
                 env: str = None,
                 logger_creator: Callable[[], Logger] = None):
        """Initialize an RLLib trainer.

        Args:
            config (dict): Algorithm-specific configuration data.
            env (str): Name of the environment to use. Note that this can also
                be specified as the `env` key in config.
            logger_creator (func): Function that creates a ray.tune.Logger
                object. If unspecified, a default logger is created.
        """

        # User provided config (this is w/o the default Trainer's
        # `COMMON_CONFIG` (see above)). Will get merged with COMMON_CONFIG
        # in self.setup().
        config = config or {}

        # Trainers allow env ids to be passed directly to the constructor.
        self._env_id = self._register_if_needed(env or config.get("env"))

        # Create a default logger creator if no logger_creator is specified
        if logger_creator is None:
            timestr = datetime.today().strftime("%Y-%m-%d_%H-%M-%S")
            logdir_prefix = "{}_{}_{}".format(self._name, self._env_id,
                                              timestr)

            def default_logger_creator(config):
                """Creates a Unified logger with a default logdir prefix
                containing the agent name and the env id
                """
                if not os.path.exists(DEFAULT_RESULTS_DIR):
                    os.makedirs(DEFAULT_RESULTS_DIR)
                logdir = tempfile.mkdtemp(
                    prefix=logdir_prefix, dir=DEFAULT_RESULTS_DIR)
                return UnifiedLogger(config, logdir, loggers=None)

            logger_creator = default_logger_creator

        super().__init__(config, logger_creator)

    @classmethod
    @override(Trainable)
    def default_resource_request(
            cls, config: PartialTrainerConfigDict) -> Resources:
        cf = dict(cls._default_config, **config)
        Trainer._validate_config(cf)
        num_workers = cf["num_workers"] + cf["evaluation_num_workers"]
        # TODO(ekl): add custom resources here once tune supports them
        return Resources(
            cpu=cf["num_cpus_for_driver"],
            gpu=cf["num_gpus"],
            memory=cf["memory"],
            object_store_memory=cf["object_store_memory"],
            extra_cpu=cf["num_cpus_per_worker"] * num_workers,
            extra_gpu=cf["num_gpus_per_worker"] * num_workers,
            extra_memory=cf["memory_per_worker"] * num_workers,
            extra_object_store_memory=cf["object_store_memory_per_worker"] *
            num_workers)

    @override(Trainable)
    @PublicAPI
    def train(self) -> ResultDict:
        """Overrides super.train to synchronize global vars."""

        result = None
        for _ in range(1 + MAX_WORKER_FAILURE_RETRIES):
            try:
                result = Trainable.train(self)
            except RayError as e:
                if self.config["ignore_worker_failures"]:
                    logger.exception(
                        "Error in train call, attempting to recover")
                    self._try_recover()
                else:
                    logger.info(
                        "Worker crashed during call to train(). To attempt to "
                        "continue training without the failed worker, set "
                        "`'ignore_worker_failures': True`.")
                    raise e
            except Exception as e:
                time.sleep(0.5)  # allow logs messages to propagate
                raise e
            else:
                break
        if result is None:
            raise RuntimeError("Failed to recover from worker crash")

        if hasattr(self, "workers") and isinstance(self.workers, WorkerSet):
            self._sync_filters_if_needed(self.workers)

        return result

    def _sync_filters_if_needed(self, workers: WorkerSet):
        if self.config.get("observation_filter", "NoFilter") != "NoFilter":
            FilterManager.synchronize(
                workers.local_worker().filters,
                workers.remote_workers(),
                update_remote=self.config["synchronize_filters"])
            logger.debug("synchronized filters: {}".format(
                workers.local_worker().filters))

    @override(Trainable)
    def log_result(self, result: ResultDict):
        self.callbacks.on_train_result(trainer=self, result=result)
        # log after the callback is invoked, so that the user has a chance
        # to mutate the result
        Trainable.log_result(self, result)

    @override(Trainable)
    def setup(self, config: PartialTrainerConfigDict):
        env = self._env_id
        if env:
            config["env"] = env
            # An already registered env.
            if _global_registry.contains(ENV_CREATOR, env):
                self.env_creator = _global_registry.get(ENV_CREATOR, env)
            # A class specifier.
            elif "." in env:
                self.env_creator = \
                    lambda env_context: from_config(env, env_context)
            # Try gym/PyBullet.
            else:

                def _creator(env_context):
                    import gym
                    # Allow for PyBullet envs to be used as well (via string).
                    # This allows for doing things like
                    # `env=CartPoleContinuousBulletEnv-v0`.
                    try:
                        import pybullet_envs
                        pybullet_envs.getList()
                    except (ModuleNotFoundError, ImportError):
                        pass
                    # Try creating a gym env. If this fails we can output a
                    # decent error message.
                    try:
                        return gym.make(env, **env_context)
                    except gym.error.Error:
                        raise ValueError(
                            "The env string you provided ({}) is a) not a "
                            "known gym/PyBullet environment specifier or b) "
                            "not registered! To register your custom envs, "
                            "do `from ray import tune; tune.register('[name]',"
                            " lambda cfg: [return actual "
                            "env from here using cfg])`. Then you can use "
                            "[name] as your config['env'].".format(env))

                self.env_creator = _creator
        else:
            self.env_creator = lambda env_config: None

        # Merge the supplied config with the class default, but store the
        # user-provided one.
        self.raw_user_config = config
        self.config = Trainer.merge_trainer_configs(self._default_config,
                                                    config)

        # Check and resolve DL framework settings.
        # Enable eager/tracing support.
        if tf1 and self.config["framework"] in ["tf2", "tfe"]:
            if self.config["framework"] == "tf2" and tfv < 2:
                raise ValueError("`framework`=tf2, but tf-version is < 2.0!")
            if not tf1.executing_eagerly():
                tf1.enable_eager_execution()
            logger.info("Executing eagerly, with eager_tracing={}".format(
                self.config["eager_tracing"]))
        if tf1 and not tf1.executing_eagerly() and \
                self.config["framework"] != "torch":
            logger.info("Tip: set framework=tfe or the --eager flag to enable "
                        "TensorFlow eager execution")

        if self.config["normalize_actions"]:
            inner = self.env_creator

            def normalize(env):
                import gym  # soft dependency
                if not isinstance(env, gym.Env):
                    raise ValueError(
                        "Cannot apply NormalizeActionActionWrapper to env of "
                        "type {}, which does not subclass gym.Env.", type(env))
                return NormalizeActionWrapper(env)

            self.env_creator = lambda env_config: normalize(inner(env_config))

        Trainer._validate_config(self.config)
        if not callable(self.config["callbacks"]):
            raise ValueError(
                "`callbacks` must be a callable method that "
                "returns a subclass of DefaultCallbacks, got {}".format(
                    self.config["callbacks"]))
        self.callbacks = self.config["callbacks"]()
        log_level = self.config.get("log_level")
        if log_level in ["WARN", "ERROR"]:
            logger.info("Current log_level is {}. For more information, "
                        "set 'log_level': 'INFO' / 'DEBUG' or use the -v and "
                        "-vv flags.".format(log_level))
        if self.config.get("log_level"):
            logging.getLogger("ray.rllib").setLevel(self.config["log_level"])

        def get_scope():
            if tf1 and not tf1.executing_eagerly():
                return tf1.Graph().as_default()
            else:
                return open(os.devnull)  # fake a no-op scope

        with get_scope():
            self._init(self.config, self.env_creator)

            # Evaluation setup.
            if self.config.get("evaluation_interval"):
                # Update env_config with evaluation settings:
                extra_config = copy.deepcopy(self.config["evaluation_config"])
                # Assert that user has not unset "in_evaluation".
                assert "in_evaluation" not in extra_config or \
                    extra_config["in_evaluation"] is True
                extra_config.update({
                    "batch_mode": "complete_episodes",
                    "rollout_fragment_length": 1,
                    "in_evaluation": True,
                })
                logger.debug(
                    "using evaluation_config: {}".format(extra_config))

                self.evaluation_workers = self._make_workers(
                    env_creator=self.env_creator,
                    validate_env=None,
                    policy_class=self._policy_class,
                    config=merge_dicts(self.config, extra_config),
                    num_workers=self.config["evaluation_num_workers"])
                self.evaluation_metrics = {}

    @override(Trainable)
    def cleanup(self):
        if hasattr(self, "workers"):
            self.workers.stop()
        if hasattr(self, "optimizer") and self.optimizer:
            self.optimizer.stop()

    @override(Trainable)
    def save_checkpoint(self, checkpoint_dir: str) -> str:
        checkpoint_path = os.path.join(checkpoint_dir,
                                       "checkpoint-{}".format(self.iteration))
        pickle.dump(self.__getstate__(), open(checkpoint_path, "wb"))

        return checkpoint_path

    @override(Trainable)
    def load_checkpoint(self, checkpoint_path: str):
        extra_data = pickle.load(open(checkpoint_path, "rb"))
        self.__setstate__(extra_data)

    @DeveloperAPI
    def _make_workers(
            self, *, env_creator: Callable[[EnvContext], EnvType],
            validate_env: Optional[Callable[[EnvType, EnvContext], None]],
            policy_class: Type[Policy], config: TrainerConfigDict,
            num_workers: int) -> WorkerSet:
        """Default factory method for a WorkerSet running under this Trainer.

        Override this method by passing a custom `make_workers` into
        `build_trainer`.

        Args:
            env_creator (callable): A function that return and Env given an env
                config.
            validate_env (Optional[Callable[[EnvType, EnvContext], None]]):
                Optional callable to validate the generated environment (only
                on worker=0).
            policy (Type[Policy]): The Policy class to use for creating the
                policies of the workers.
            config (TrainerConfigDict): The Trainer's config.
            num_workers (int): Number of remote rollout workers to create.
                0 for local only.

        Returns:
            WorkerSet: The created WorkerSet.
        """
        return WorkerSet(
            env_creator=env_creator,
            validate_env=validate_env,
            policy_class=policy_class,
            trainer_config=config,
            num_workers=num_workers,
            logdir=self.logdir)

    @DeveloperAPI
    def _init(self, config: TrainerConfigDict,
              env_creator: Callable[[EnvContext], EnvType]):
        """Subclasses should override this for custom initialization."""
        raise NotImplementedError

    @DeveloperAPI
    def _evaluate(self) -> dict:
        """Evaluates current policy under `evaluation_config` settings.

        Note that this default implementation does not do anything beyond
        merging evaluation_config with the normal trainer config.
        """
        # Call the `_before_evaluate` hook.
        self._before_evaluate()
        # Sync weights to the evaluation WorkerSet.
        self._sync_weights_to_workers(worker_set=self.evaluation_workers)
        self._sync_filters_if_needed(self.evaluation_workers)

        if self.config["custom_eval_function"]:
            logger.info("Running custom eval function {}".format(
                self.config["custom_eval_function"]))
            metrics = self.config["custom_eval_function"](
                self, self.evaluation_workers)
            if not metrics or not isinstance(metrics, dict):
                raise ValueError("Custom eval function must return "
                                 "dict of metrics, got {}.".format(metrics))
        else:
            logger.info("Evaluating current policy for {} episodes.".format(
                self.config["evaluation_num_episodes"]))
            if self.config["evaluation_num_workers"] == 0:
                for _ in range(self.config["evaluation_num_episodes"]):
                    self.evaluation_workers.local_worker().sample()
            else:
                num_rounds = int(
                    math.ceil(self.config["evaluation_num_episodes"] /
                              self.config["evaluation_num_workers"]))
                num_workers = len(self.evaluation_workers.remote_workers())
                num_episodes = num_rounds * num_workers
                for i in range(num_rounds):
                    logger.info("Running round {} of parallel evaluation "
                                "({}/{} episodes)".format(
                                    i, (i + 1) * num_workers, num_episodes))
                    ray.get([
                        w.sample.remote()
                        for w in self.evaluation_workers.remote_workers()
                    ])

            metrics = collect_metrics(self.evaluation_workers.local_worker(),
                                      self.evaluation_workers.remote_workers())
        return {"evaluation": metrics}

    @DeveloperAPI
    def _before_evaluate(self):
        """Pre-evaluation callback."""
        pass

    @DeveloperAPI
    def _sync_weights_to_workers(
            self,
            *,
            worker_set: Optional[WorkerSet] = None,
            workers: Optional[List[RolloutWorker]] = None,
    ) -> None:
        """Sync "main" weights to given WorkerSet or list of workers."""
        assert worker_set is not None
        # Broadcast the new policy weights to all evaluation workers.
        logger.info("Synchronizing weights to evaluation workers.")
        weights = ray.put(self.workers.local_worker().save())
        worker_set.foreach_worker(lambda w: w.restore(ray.get(weights)))

    @PublicAPI
    def compute_action(self,
                       observation: TensorStructType,
                       state: List[TensorStructType] = None,
                       prev_action: TensorStructType = None,
                       prev_reward: float = None,
                       info: EnvInfoDict = None,
                       policy_id: PolicyID = DEFAULT_POLICY_ID,
                       full_fetch: bool = False,
                       explore: bool = None) -> TensorStructType:
        """Computes an action for the specified policy on the local Worker.

        Note that you can also access the policy object through
        self.get_policy(policy_id) and call compute_actions() on it directly.

        Args:
            observation (TensorStructType): observation from the environment.
            state (List[TensorStructType]): RNN hidden state, if any. If state
                is not None, then all of compute_single_action(...) is returned
                (computed action, rnn state(s), logits dictionary).
                Otherwise compute_single_action(...)[0] is returned
                (computed action).
            prev_action (TensorStructType): Previous action value, if any.
            prev_reward (float): Previous reward, if any.
            info (EnvInfoDict): info object, if any
            policy_id (PolicyID): Policy to query (only applies to
                multi-agent).
            full_fetch (bool): Whether to return extra action fetch results.
                This is always set to True if RNN state is specified.
            explore (bool): Whether to pick an exploitation or exploration
                action (default: None -> use self.config["explore"]).

        Returns:
            any: The computed action if full_fetch=False, or
            tuple: The full output of policy.compute_actions() if
                full_fetch=True or we have an RNN-based Policy.
        """
        if state is None:
            state = []
        # Check the preprocessor and preprocess, if necessary.
        pp = self.workers.local_worker().preprocessors[policy_id]
        if type(pp).__name__ != "NoPreprocessor":
            observation = pp.transform(observation)
        filtered_observation = self.workers.local_worker().filters[policy_id](
            observation, update=False)

        result = self.get_policy(policy_id).compute_single_action(
            filtered_observation,
            state,
            prev_action,
            prev_reward,
            info,
            clip_actions=self.config["clip_actions"],
            explore=explore)

        if state or full_fetch:
            return result
        else:
            return result[0]  # backwards compatibility

    def compute_actions(self,
                        observations,
                        state=None,
                        prev_action=None,
                        prev_reward=None,
                        info=None,
                        policy_id=DEFAULT_POLICY_ID,
                        full_fetch=False,
                        explore=None):
        """Computes an action for the specified policy on the local Worker.

        Note that you can also access the policy object through
        self.get_policy(policy_id) and call compute_actions() on it directly.

        Args:
            observation (obj): observation from the environment.
            state (dict): RNN hidden state, if any. If state is not None,
                then all of compute_single_action(...) is returned
                (computed action, rnn state(s), logits dictionary).
                Otherwise compute_single_action(...)[0] is returned
                (computed action).
            prev_action (obj): previous action value, if any
            prev_reward (int): previous reward, if any
            info (dict): info object, if any
            policy_id (str): Policy to query (only applies to multi-agent).
            full_fetch (bool): Whether to return extra action fetch results.
                This is always set to True if RNN state is specified.
            explore (bool): Whether to pick an exploitation or exploration
                action (default: None -> use self.config["explore"]).

        Returns:
            any: The computed action if full_fetch=False, or
            tuple: The full output of policy.compute_actions() if
                full_fetch=True or we have an RNN-based Policy.
        """
        # Preprocess obs and states
        stateDefined = state is not None
        policy = self.get_policy(policy_id)
        filtered_obs, filtered_state = [], []
        for agent_id, ob in observations.items():
            worker = self.workers.local_worker()
            preprocessed = worker.preprocessors[policy_id].transform(ob)
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

        # Batch compute actions
        actions, states, infos = policy.compute_actions(
            obs_batch,
            state,
            prev_action,
            prev_reward,
            info,
            clip_actions=self.config["clip_actions"],
            explore=explore)

        # Unbatch actions for the environment
        atns, actions = space_utils.unbatch(actions), {}
        for key, atn in zip(observations, atns):
            actions[key] = atn

        # Unbatch states into a dict
        unbatched_states = {}
        for idx, agent_id in enumerate(observations):
            unbatched_states[agent_id] = [s[idx] for s in states]

        # Return only actions or full tuple
        if stateDefined or full_fetch:
            return actions, unbatched_states, infos
        else:
            return actions

    @property
    def _name(self) -> str:
        """Subclasses should override this to declare their name."""
        raise NotImplementedError

    @property
    def _default_config(self) -> TrainerConfigDict:
        """Subclasses should override this to declare their default config."""
        raise NotImplementedError

    @PublicAPI
    def get_policy(self, policy_id: PolicyID = DEFAULT_POLICY_ID) -> Policy:
        """Return policy for the specified id, or None.

        Args:
            policy_id (str): id of policy to return.
        """
        return self.workers.local_worker().get_policy(policy_id)

    @PublicAPI
    def get_weights(self, policies: List[PolicyID] = None) -> dict:
        """Return a dictionary of policy ids to weights.

        Args:
            policies (list): Optional list of policies to return weights for,
                or None for all policies.
        """
        return self.workers.local_worker().get_weights(policies)

    @PublicAPI
    def set_weights(self, weights: Dict[PolicyID, dict]):
        """Set policy weights by policy id.

        Args:
            weights (dict): Map of policy ids to weights to set.
        """
        self.workers.local_worker().set_weights(weights)

    @DeveloperAPI
    def export_policy_model(self,
                            export_dir: str,
                            policy_id: PolicyID = DEFAULT_POLICY_ID):
        """Export policy model with given policy_id to local directory.

        Args:
            export_dir (string): Writable local directory.
            policy_id (string): Optional policy id to export.

        Example:
            >>> trainer = MyTrainer()
            >>> for _ in range(10):
            >>>     trainer.train()
            >>> trainer.export_policy_model("/tmp/export_dir")
        """
        self.workers.local_worker().export_policy_model(export_dir, policy_id)

    @DeveloperAPI
    def export_policy_checkpoint(self,
                                 export_dir: str,
                                 filename_prefix: str = "model",
                                 policy_id: PolicyID = DEFAULT_POLICY_ID):
        """Export tensorflow policy model checkpoint to local directory.

        Args:
            export_dir (string): Writable local directory.
            filename_prefix (string): file name prefix of checkpoint files.
            policy_id (string): Optional policy id to export.

        Example:
            >>> trainer = MyTrainer()
            >>> for _ in range(10):
            >>>     trainer.train()
            >>> trainer.export_policy_checkpoint("/tmp/export_dir")
        """
        self.workers.local_worker().export_policy_checkpoint(
            export_dir, filename_prefix, policy_id)

    @DeveloperAPI
    def import_policy_model_from_h5(self,
                                    import_file: str,
                                    policy_id: PolicyID = DEFAULT_POLICY_ID):
        """Imports a policy's model with given policy_id from a local h5 file.

        Args:
            import_file (str): The h5 file to import from.
            policy_id (string): Optional policy id to import into.

        Example:
            >>> trainer = MyTrainer()
            >>> trainer.import_policy_model_from_h5("/tmp/weights.h5")
            >>> for _ in range(10):
            >>>     trainer.train()
        """
        self.workers.local_worker().import_policy_model_from_h5(
            import_file, policy_id)

    @DeveloperAPI
    def collect_metrics(self,
                        selected_workers: List["ActorHandle"] = None) -> dict:
        """Collects metrics from the remote workers of this agent.

        This is the same data as returned by a call to train().
        """
        return self.optimizer.collect_metrics(
            self.config["collect_metrics_timeout"],
            min_history=self.config["metrics_smoothing_episodes"],
            selected_workers=selected_workers)

    @classmethod
    def resource_help(cls, config: TrainerConfigDict) -> str:
        return ("\n\nYou can adjust the resource requests of RLlib agents by "
                "setting `num_workers`, `num_gpus`, and other configs. See "
                "the DEFAULT_CONFIG defined by each agent for more info.\n\n"
                "The config of this agent is: {}".format(config))

    @classmethod
    def merge_trainer_configs(cls,
                              config1: TrainerConfigDict,
                              config2: PartialTrainerConfigDict,
                              _allow_unknown_configs: Optional[bool] = None
                              ) -> TrainerConfigDict:
        config1 = copy.deepcopy(config1)
        if "callbacks" in config2 and type(config2["callbacks"]) is dict:
            legacy_callbacks_dict = config2["callbacks"]

            def make_callbacks():
                # Deprecation warning will be logged by DefaultCallbacks.
                return DefaultCallbacks(
                    legacy_callbacks_dict=legacy_callbacks_dict)

            config2["callbacks"] = make_callbacks
        if _allow_unknown_configs is None:
            _allow_unknown_configs = cls._allow_unknown_configs
        return deep_update(config1, config2, _allow_unknown_configs,
                           cls._allow_unknown_subkeys,
                           cls._override_all_subkeys_if_type_changes)

    @staticmethod
    def _validate_config(config: PartialTrainerConfigDict):
        model_config = config.get("model")
        if model_config is None:
            config["model"] = model_config = {}

        if not config.get("_use_trajectory_view_api"):
            traj_view_framestacks = model_config.get("num_framestacks", "auto")
            if model_config.get("_time_major"):
                raise ValueError("`model._time_major` only supported "
                                 "iff `_use_trajectory_view_api` is True!")
            elif traj_view_framestacks != "auto":
                raise ValueError("`model.num_framestacks` only supported "
                                 "iff `_use_trajectory_view_api` is True!")
            model_config["num_framestacks"] = 0

        if isinstance(config["input_evaluation"], tuple):
            config["input_evaluation"] = list(config["input_evaluation"])
        elif not isinstance(config["input_evaluation"], list):
            raise ValueError(
                "`input_evaluation` must be a list of strings, got {}!".format(
                    config["input_evaluation"]))

        # Check model config.
        prev_a_r = model_config.get("lstm_use_prev_action_reward",
                                    DEPRECATED_VALUE)
        if prev_a_r != DEPRECATED_VALUE:
            deprecation_warning(
                "model.lstm_use_prev_action_reward",
                "model.lstm_use_prev_action and model.lstm_use_prev_reward",
                error=False)
            model_config["lstm_use_prev_action"] = prev_a_r
            model_config["lstm_use_prev_reward"] = prev_a_r

        # Check batching/sample collection settings.
        if config["batch_mode"] not in [
                "truncate_episodes", "complete_episodes"
        ]:
            raise ValueError("`batch_mode` must be one of [truncate_episodes|"
                             "complete_episodes]! Got {}".format(
                                 config["batch_mode"]))

        if config["multiagent"].get("count_steps_by", "env_steps") not in \
                ["env_steps", "agent_steps"]:
            raise ValueError(
                "`count_steps_by` must be one of [env_steps|agent_steps]! "
                "Got {}".format(config["multiagent"]["count_steps_by"]))

        # If evaluation_num_workers > 0, warn if evaluation_interval is None
        # (also set it to 1).
        if config["evaluation_num_workers"] > 0 and \
                not config["evaluation_interval"]:
            logger.warning(
                "You have specified {} evaluation workers, but no evaluation "
                "interval! Will set the interval to 1 (each `train()` call). "
                "If this is too frequent, set `evaluation_interval` to some "
                "larger value.".format(config["evaluation_num_workers"]))
            config["evaluation_interval"] = 1

    def _try_recover(self):
        """Try to identify and remove any unhealthy workers.

        This method is called after an unexpected remote error is encountered
        from a worker. It issues check requests to all current workers and
        removes any that respond with error. If no healthy workers remain,
        an error is raised.
        """

        assert hasattr(self, "execution_plan")
        workers = self.workers

        logger.info("Health checking all workers...")
        checks = []
        for ev in workers.remote_workers():
            _, obj_ref = ev.sample_with_count.remote()
            checks.append(obj_ref)

        healthy_workers = []
        for i, obj_ref in enumerate(checks):
            w = workers.remote_workers()[i]
            try:
                ray.get(obj_ref)
                healthy_workers.append(w)
                logger.info("Worker {} looks healthy".format(i + 1))
            except RayError:
                logger.exception("Removing unhealthy worker {}".format(i + 1))
                try:
                    w.__ray_terminate__.remote()
                except Exception:
                    logger.exception("Error terminating unhealthy worker")

        if len(healthy_workers) < 1:
            raise RuntimeError(
                "Not enough healthy workers remain to continue.")

        logger.warning("Recreating execution plan after failure")
        workers.reset(healthy_workers)
        self.train_exec_impl = self.execution_plan(workers, self.config)

    @override(Trainable)
    def _export_model(self, export_formats: List[str],
                      export_dir: str) -> Dict[str, str]:
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
        return exported

    def import_model(self, import_file: str):
        """Imports a model from import_file.

        Note: Currently, only h5 files are supported.

        Args:
            import_file (str): The file to import the model from.

        Returns:
            A dict that maps ExportFormats to successfully exported models.
        """
        # Check for existence.
        if not os.path.exists(import_file):
            raise FileNotFoundError(
                "`import_file` '{}' does not exist! Can't import Model.".
                format(import_file))
        # Get the format of the given file.
        import_format = "h5"  # TODO(sven): Support checkpoint loading.

        ExportFormat.validate([import_format])
        if import_format != ExportFormat.H5:
            raise NotImplementedError
        else:
            return self.import_policy_model_from_h5(import_file)

    def __getstate__(self) -> dict:
        state = {}
        if hasattr(self, "workers"):
            state["worker"] = self.workers.local_worker().save()
        if hasattr(self, "optimizer") and hasattr(self.optimizer, "save"):
            state["optimizer"] = self.optimizer.save()
        return state

    def __setstate__(self, state: dict):
        if "worker" in state:
            self.workers.local_worker().restore(state["worker"])
            remote_state = ray.put(state["worker"])
            for r in self.workers.remote_workers():
                r.restore.remote(remote_state)
        if "optimizer" in state:
            self.optimizer.restore(state["optimizer"])

    @staticmethod
    def with_updates(**overrides) -> Type["Trainer"]:
        raise NotImplementedError(
            "`with_updates` may only be called on Trainer sub-classes "
            "that were generated via the `ray.rllib.agents.trainer_template."
            "build_trainer()` function!")

    def _register_if_needed(self, env_object: Union[str, EnvType]):
        if isinstance(env_object, str):
            return env_object
        elif isinstance(env_object, type):
            name = env_object.__name__
            register_env(name, lambda config: env_object(config))
            return name
        raise ValueError(
            "{} is an invalid env specification. ".format(env_object) +
            "You can specify a custom env as either a class "
            "(e.g., YourEnvCls) or a registered env id (e.g., \"your_env\").")

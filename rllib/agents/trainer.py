from collections import defaultdict
import concurrent
import copy
from datetime import datetime
import functools
import gym
import logging
import math
import numpy as np
import os
import pickle
import tempfile
import time
from typing import Callable, DefaultDict, Dict, List, Optional, Set, Tuple, \
    Type, Union

import ray
from ray.actor import ActorHandle
from ray.exceptions import RayError
from ray.rllib.agents.callbacks import DefaultCallbacks
from ray.rllib.env.env_context import EnvContext
from ray.rllib.env.multi_agent_env import MultiAgentEnv
from ray.rllib.env.utils import gym_env_creator
from ray.rllib.evaluation.collectors.simple_list_collector import \
    SimpleListCollector
from ray.rllib.evaluation.episode import Episode
from ray.rllib.evaluation.metrics import collect_episodes, collect_metrics, \
    summarize_episodes
from ray.rllib.evaluation.rollout_worker import RolloutWorker
from ray.rllib.evaluation.worker_set import WorkerSet
from ray.rllib.execution.metric_ops import StandardMetricsReporting
from ray.rllib.execution.buffers.multi_agent_replay_buffer import \
    MultiAgentReplayBuffer
from ray.rllib.execution.common import WORKER_UPDATE_TIMER
from ray.rllib.execution.rollout_ops import ConcatBatches, ParallelRollouts, \
    synchronous_parallel_sample
from ray.rllib.execution.train_ops import TrainOneStep, MultiGPUTrainOneStep, \
    train_one_step, multi_gpu_train_one_step
from ray.rllib.models import MODEL_DEFAULTS
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID, SampleBatch
from ray.rllib.utils import deep_update, FilterManager, merge_dicts
from ray.rllib.utils.annotations import DeveloperAPI, ExperimentalAPI, \
    override, PublicAPI
from ray.rllib.utils.debug import update_global_seed_if_necessary
from ray.rllib.utils.deprecation import Deprecated, deprecation_warning, \
    DEPRECATED_VALUE
from ray.rllib.utils.error import EnvError, ERR_MSG_INVALID_ENV_DESCRIPTOR
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.from_config import from_config
from ray.rllib.utils.metrics import NUM_ENV_STEPS_SAMPLED, \
    NUM_AGENT_STEPS_SAMPLED, NUM_ENV_STEPS_TRAINED, NUM_AGENT_STEPS_TRAINED
from ray.rllib.utils.metrics.learner_info import LEARNER_INFO
from ray.rllib.utils.pre_checks.multi_agent import check_multi_agent
from ray.rllib.utils.spaces import space_utils
from ray.rllib.utils.typing import AgentID, EnvInfoDict, EnvType, EpisodeID, \
    PartialTrainerConfigDict, PolicyID, ResultDict, TensorStructType, \
    TensorType, TrainerConfigDict
from ray.tune.logger import Logger, UnifiedLogger
from ray.tune.registry import ENV_CREATOR, register_env, _global_registry
from ray.tune.resources import Resources
from ray.tune.result import DEFAULT_RESULTS_DIR
from ray.tune.trainable import Trainable
from ray.tune.trial import ExportFormat
from ray.tune.utils.placement_groups import PlacementGroupFactory
from ray.util import log_once
from ray.util.timer import _Timer

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
    # Number of environments to evaluate vector-wise per worker. This enables
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
    # Discount factor of the MDP.
    "gamma": 0.99,
    # The default learning rate.
    "lr": 0.0001,
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
    # Number of steps after which the episode is forced to terminate. Defaults
    # to `env.spec.max_episode_steps` (if present) for Gym envs.
    "horizon": None,
    # Calculate rewards but don't reset the environment when the horizon is
    # hit. This allows value estimation and RNN state to span across logical
    # episodes denoted by horizon. This only has an effect if horizon != inf.
    "soft_horizon": False,
    # Don't set 'done' at the end of the episode.
    # In combination with `soft_horizon`, this works as follows:
    # - no_done_at_end=False soft_horizon=False:
    #   Reset env and add `done=True` at end of each episode.
    # - no_done_at_end=True soft_horizon=False:
    #   Reset env, but do NOT add `done=True` at end of the episode.
    # - no_done_at_end=False soft_horizon=True:
    #   Do NOT reset env at horizon, but add `done=True` at the horizon
    #   (pretending the episode has terminated).
    # - no_done_at_end=True soft_horizon=True:
    #   Do NOT reset env at horizon and do NOT add `done=True` at the horizon.
    "no_done_at_end": False,
    # The environment specifier:
    # This can either be a tune-registered env, via
    # `tune.register_env([name], lambda env_ctx: [env object])`,
    # or a string specifier of an RLlib supported type. In the latter case,
    # RLlib will try to interpret the specifier as either an openAI gym env,
    # a PyBullet env, a ViZDoomGym env, or a fully qualified classpath to an
    # Env class, e.g. "ray.rllib.examples.env.random_env.RandomEnv".
    "env": None,
    # The observation- and action spaces for the Policies of this Trainer.
    # Use None for automatically inferring these from the given env.
    "observation_space": None,
    "action_space": None,
    # Arguments dict passed to the env creator as an EnvContext object (which
    # is a dict plus the properties: num_workers, worker_index, vector_index,
    # and remote).
    "env_config": {},
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
    # A callable taking the last train results, the base env and the env
    # context as args and returning a new task to set the env to.
    # The env must be a `TaskSettableEnv` sub-class for this to work.
    # See `examples/curriculum_learning.py` for an example.
    "env_task_fn": None,
    # If True, try to render the environment on the local worker or on worker
    # 1 (if num_workers > 0). For vectorized envs, this usually means that only
    # the first sub-environment will be rendered.
    # In order for this to work, your env will have to implement the
    # `render()` method which either:
    # a) handles window generation and rendering itself (returning True) or
    # b) returns a numpy uint8 image of shape [height x width x 3 (RGB)].
    "render_env": False,
    # If True, stores videos in this relative directory inside the default
    # output dir (~/ray_results/...). Alternatively, you can specify an
    # absolute path (str), in which the env recordings should be
    # stored instead.
    # Set to False for not recording anything.
    # Note: This setting replaces the deprecated `monitor` key.
    "record_env": False,
    # Whether to clip rewards during Policy's postprocessing.
    # None (default): Clip for Atari only (r=sign(r)).
    # True: r=sign(r): Fixed rewards -1.0, 1.0, or 0.0.
    # False: Never clip.
    # [float value]: Clip at -value and + value.
    # Tuple[value1, value2]: Clip at value1 and value2.
    "clip_rewards": None,
    # If True, RLlib will learn entirely inside a normalized action space
    # (0.0 centered with small stddev; only affecting Box components).
    # We will unsquash actions (and clip, just in case) to the bounds of
    # the env's action space before sending actions back to the env.
    "normalize_actions": True,
    # If True, RLlib will clip actions according to the env's bounds
    # before sending them back to the env.
    # TODO: (sven) This option should be obsoleted and always be False.
    "clip_actions": False,
    # Whether to use "rllib" or "deepmind" preprocessors by default
    # Set to None for using no preprocessor. In this case, the model will have
    # to handle possibly complex observations from the environment.
    "preprocessor_pref": "deepmind",

    # === Debug Settings ===
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
    # tf: TensorFlow (static-graph)
    # tf2: TensorFlow 2.x (eager or traced, if eager_tracing=True)
    # tfe: TensorFlow eager (or traced, if eager_tracing=True)
    # torch: PyTorch
    "framework": "tf",
    # Enable tracing in eager mode. This greatly improves performance
    # (speedup ~2x), but makes it slightly harder to debug since Python
    # code won't be evaluated after the initial eager pass.
    # Only possible if framework=[tf2|tfe].
    "eager_tracing": False,
    # Maximum number of tf.function re-traces before a runtime error is raised.
    # This is to prevent unnoticed retraces of methods inside the
    # `..._eager_traced` Policy, which could slow down execution by a
    # factor of 4, without the user noticing what the root cause for this
    # slowdown could be.
    # Only necessary for framework=[tf2|tfe].
    # Set to None to ignore the re-trace count and never throw an error.
    "eager_max_retraces": 20,

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
    # Note that for Ape-X metrics are already only reported for the lowest
    # epsilon workers (least random workers).
    # Set to None (or 0) for no evaluation.
    "evaluation_interval": None,
    # Duration for which to run evaluation each `evaluation_interval`.
    # The unit for the duration can be set via `evaluation_duration_unit` to
    # either "episodes" (default) or "timesteps".
    # If using multiple evaluation workers (evaluation_num_workers > 1),
    # the load to run will be split amongst these.
    # If the value is "auto":
    # - For `evaluation_parallel_to_training=True`: Will run as many
    #   episodes/timesteps that fit into the (parallel) training step.
    # - For `evaluation_parallel_to_training=False`: Error.
    "evaluation_duration": 10,
    # The unit, with which to count the evaluation duration. Either "episodes"
    # (default) or "timesteps".
    "evaluation_duration_unit": "episodes",
    # Whether to run evaluation in parallel to a Trainer.train() call
    # using threading. Default=False.
    # E.g. evaluation_interval=2 -> For every other training iteration,
    # the Trainer.train() and Trainer.evaluate() calls run in parallel.
    # Note: This is experimental. Possible pitfalls could be race conditions
    # for weight synching at the beginning of the evaluation loop.
    "evaluation_parallel_to_training": False,
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
    # process (only if evaluation_interval is not None). If you increase this,
    # it will increase the Ray resource usage of the trainer since evaluation
    # workers are created separately from rollout workers (used to sample data
    # for training).
    "evaluation_num_workers": 0,
    # Customize the evaluation method. This must be a function of signature
    # (trainer: Trainer, eval_workers: WorkerSet) -> metrics: dict. See the
    # Trainer.evaluate() method to see the default implementation.
    # The Trainer guarantees all eval workers have the latest policy state
    # before this function is called.
    "custom_eval_function": None,
    # Make sure the latest available evaluation results are always attached to
    # a step result dict.
    # This may be useful if Tune or some other meta controller needs access
    # to evaluation metrics all the time.
    "always_attach_evaluation_results": False,

    # === Advanced Rollout Settings ===
    # Use a background thread for sampling (slightly off-policy, usually not
    # advisable to turn on unless your env specifically requires it).
    "sample_async": False,

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
        # Required by multi-GPU (num_gpus > 1).
        "allow_soft_placement": True,
    },
    # Override the following tf session args on the local worker
    "local_tf_session_args": {
        # Allow a higher level of parallelism by default, but not unlimited
        # since that can cause crashes with many concurrent drivers.
        "intra_op_parallelism_threads": 8,
        "inter_op_parallelism_threads": 8,
    },
    # Whether to LZ4 compress individual observations.
    "compress_observations": False,
    # Wait for metric batches for at most this many seconds. Those that
    # have not returned in time will be collected in the next train iteration.
    "metrics_episode_collection_timeout_s": 180,
    # Smooth metrics over this many episodes.
    "metrics_num_episodes_for_smoothing": 100,
    # Minimum time interval to run one `train()` call for:
    # If - after one `step_attempt()`, this time limit has not been reached,
    # will perform n more `step_attempt()` calls until this minimum time has
    # been consumed. Set to None or 0 for no minimum time.
    "min_time_s_per_reporting": None,
    # Minimum train/sample timesteps to optimize for per `train()` call.
    # This value does not affect learning, only the length of train iterations.
    # If - after one `step_attempt()`, the timestep counts (sampling or
    # training) have not been reached, will perform n more `step_attempt()`
    # calls until the minimum timesteps have been executed.
    # Set to None or 0 for no minimum timesteps.
    "min_train_timesteps_per_reporting": None,
    "min_sample_timesteps_per_reporting": None,

    # This argument, in conjunction with worker_index, sets the random seed of
    # each worker, so that identically configured trials will have identical
    # results. This makes experiments reproducible.
    "seed": None,
    # Any extra python env vars to set in the trainer process, e.g.,
    # {"OMP_NUM_THREADS": "16"}
    "extra_python_environs_for_driver": {},
    # The extra python environments need to set for worker processes.
    "extra_python_environs_for_worker": {},

    # === Resource Settings ===
    # Number of GPUs to allocate to the trainer process. Note that not all
    # algorithms can take advantage of trainer GPUs. Support for multi-GPU
    # is currently only available for tf-[PPO/IMPALA/DQN/PG].
    # This can be fractional (e.g., 0.3 GPUs).
    "num_gpus": 0,
    # Set to True for debugging (multi-)?GPU funcitonality on a CPU machine.
    # GPU towers will be simulated by graphs located on CPUs in this case.
    # Use `num_gpus` to test for different numbers of fake GPUs.
    "_fake_gpus": False,
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
    # The strategy for the placement group factory returned by
    # `Trainer.default_resource_request()`. A PlacementGroup defines, which
    # devices (resources) should always be co-located on the same node.
    # For example, a Trainer with 2 rollout workers, running with
    # num_gpus=1 will request a placement group with the bundles:
    # [{"gpu": 1, "cpu": 1}, {"cpu": 1}, {"cpu": 1}], where the first bundle is
    # for the driver and the other 2 bundles are for the two workers.
    # These bundles can now be "placed" on the same or different
    # nodes depending on the value of `placement_strategy`:
    # "PACK": Packs bundles into as few nodes as possible.
    # "SPREAD": Places bundles across distinct nodes as even as possible.
    # "STRICT_PACK": Packs bundles into one node. The group is not allowed
    #   to span multiple nodes.
    # "STRICT_SPREAD": Packs bundles across distinct nodes.
    "placement_strategy": "PACK",

    # === Offline Datasets ===
    # Specify how to generate experiences:
    #  - "sampler": Generate experiences via online (env) simulation (default).
    #  - A local directory or file glob expression (e.g., "/tmp/*.json").
    #  - A list of individual file paths/URIs (e.g., ["/tmp/1.json",
    #    "s3://bucket/2.json"]).
    #  - A dict with string keys and sampling probabilities as values (e.g.,
    #    {"sampler": 0.4, "/tmp/*.json": 0.4, "s3://bucket/expert.json": 0.2}).
    #  - A callable that takes an `IOContext` object as only arg and returns a
    #    ray.rllib.offline.InputReader.
    #  - A string key that indexes a callable with tune.registry.register_input
    "input": "sampler",
    # Arguments accessible from the IOContext for configuring custom input
    "input_config": {},
    # True, if the actions in a given offline "input" are already normalized
    # (between -1.0 and 1.0). This is usually the case when the offline
    # file has been generated by another RLlib algorithm (e.g. PPO or SAC),
    # while "normalize_actions" was set to True.
    "actions_in_input_normalized": False,
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
        # Keep this many policies in the "policy_map" (before writing
        # least-recently used ones to disk/S3).
        "policy_map_capacity": 100,
        # Where to store overflowing (least-recently used) policies?
        # Could be a directory (str) or an S3 location. None for using
        # the default output dir.
        "policy_map_cache": None,
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

    # === API deprecations/simplifications/changes ===
    # Experimental flag.
    # If True, TFPolicy will handle more than one loss/optimizer.
    # Set this to True, if you would like to return more than
    # one loss term from your `loss_fn` and an equal number of optimizers
    # from your `optimizer_fn`.
    # In the future, the default for this will be True.
    "_tf_policy_handles_more_than_one_loss": False,
    # Experimental flag.
    # If True, no (observation) preprocessor will be created and
    # observations will arrive in model as they are returned by the env.
    # In the future, the default for this will be True.
    "_disable_preprocessor_api": False,
    # Experimental flag.
    # If True, RLlib will no longer flatten the policy-computed actions into
    # a single tensor (for storage in SampleCollectors/output files/etc..),
    # but leave (possibly nested) actions as-is. Disabling flattening affects:
    # - SampleCollectors: Have to store possibly nested action structs.
    # - Models that have the previous action(s) as part of their input.
    # - Algorithms reading from offline files (incl. action information).
    "_disable_action_flattening": False,
    # Experimental flag.
    # If True, the execution plan API will not be used. Instead,
    # a Trainer's `training_iteration` method will be called as-is each
    # training iteration.
    "_disable_execution_plan_api": False,

    # === Deprecated keys ===
    # Uses the sync samples optimizer instead of the multi-gpu one. This is
    # usually slower, but you might want to try it if you run into issues with
    # the default optimizer.
    # This will be set automatically from now on.
    "simple_optimizer": DEPRECATED_VALUE,
    # Whether to write episode stats and videos to the agent log dir. This is
    # typically located in ~/ray_results.
    "monitor": DEPRECATED_VALUE,
    # Replaced by `evaluation_duration=10` and
    # `evaluation_duration_unit=episodes`.
    "evaluation_num_episodes": DEPRECATED_VALUE,
    # Use `metrics_num_episodes_for_smoothing` instead.
    "metrics_smoothing_episodes": DEPRECATED_VALUE,
    # Use `min_[env|train]_timesteps_per_reporting` instead.
    "timesteps_per_iteration": 0,
    # Use `min_time_s_per_reporting` instead.
    "min_iter_time_s": DEPRECATED_VALUE,
    # Use `metrics_episode_collection_timeout_s` instead.
    "collect_metrics_timeout": DEPRECATED_VALUE,
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
    """An RLlib algorithm responsible for optimizing one or more Policies.

    Trainers contain a WorkerSet under `self.workers`. A WorkerSet is
    normally composed of a single local worker
    (self.workers.local_worker()), used to compute and apply learning updates,
    and optionally one or more remote workers (self.workers.remote_workers()),
    used to generate environment samples in parallel.

    Each worker (remotes or local) contains a PolicyMap, which itself
    may contain either one policy for single-agent training or one or more
    policies for multi-agent training. Policies are synchronized
    automatically from time to time using ray.remote calls. The exact
    synchronization logic depends on the specific algorithm (Trainer) used,
    but this usually happens from local worker to all remote workers and
    after each training update.

    You can write your own Trainer classes by sub-classing from `Trainer`
    or any of its built-in sub-classes.
    This allows you to override the `execution_plan` method to implement
    your own algorithm logic. You can find the different built-in
    algorithms' execution plans in their respective main py files,
    e.g. rllib.agents.dqn.dqn.py or rllib.agents.impala.impala.py.

    The most important API methods a Trainer exposes are `train()`,
    `evaluate()`, `save()` and `restore()`. Trainer objects retain internal
    model state between calls to train(), so you should create a new
    Trainer instance for each training session.
    """

    # Whether to allow unknown top-level config keys.
    _allow_unknown_configs = False

    # List of top-level keys with value=dict, for which new sub-keys are
    # allowed to be added to the value dict.
    _allow_unknown_subkeys = [
        "tf_session_args", "local_tf_session_args", "env_config", "model",
        "optimizer", "multiagent", "custom_resources_per_worker",
        "evaluation_config", "exploration_config",
        "extra_python_environs_for_driver", "extra_python_environs_for_worker",
        "input_config"
    ]

    # List of top level keys with value=dict, for which we always override the
    # entire value (dict), iff the "type" key in that value dict changes.
    _override_all_subkeys_if_type_changes = ["exploration_config"]

    # TODO: Deprecate. Instead, override `Trainer.get_default_config()`.
    _default_config = COMMON_CONFIG

    @PublicAPI
    def __init__(self,
                 config: Optional[PartialTrainerConfigDict] = None,
                 env: Optional[Union[str, EnvType]] = None,
                 logger_creator: Optional[Callable[[], Logger]] = None,
                 remote_checkpoint_dir: Optional[str] = None,
                 sync_function_tpl: Optional[str] = None):
        """Initializes a Trainer instance.

        Args:
            config: Algorithm-specific configuration dict.
            env: Name of the environment to use (e.g. a gym-registered str),
                a full class path (e.g.
                "ray.rllib.examples.env.random_env.RandomEnv"), or an Env
                class directly. Note that this arg can also be specified via
                the "env" key in `config`.
            logger_creator: Callable that creates a ray.tune.Logger
                object. If unspecified, a default logger is created.
        """

        # User provided (partial) config (this may be w/o the default
        # Trainer's `COMMON_CONFIG` (see above)). Will get merged with
        # COMMON_CONFIG in self.setup().
        config = config or {}

        # Trainers allow env ids to be passed directly to the constructor.
        self._env_id = self._register_if_needed(
            env or config.get("env"), config)
        # The env creator callable, taking an EnvContext (config dict)
        # as arg and returning an RLlib supported Env type (e.g. a gym.Env).
        self.env_creator: Callable[[EnvContext], EnvType] = None

        # Placeholder for a local replay buffer instance.
        self.local_replay_buffer = None

        # Create a default logger creator if no logger_creator is specified
        if logger_creator is None:
            # Default logdir prefix containing the agent's name and the
            # env id.
            timestr = datetime.today().strftime("%Y-%m-%d_%H-%M-%S")
            logdir_prefix = "{}_{}_{}".format(str(self), self._env_id, timestr)
            if not os.path.exists(DEFAULT_RESULTS_DIR):
                os.makedirs(DEFAULT_RESULTS_DIR)
            logdir = tempfile.mkdtemp(
                prefix=logdir_prefix, dir=DEFAULT_RESULTS_DIR)

            # Allow users to more precisely configure the created logger
            # via "logger_config.type".
            if config.get(
                    "logger_config") and "type" in config["logger_config"]:

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

        # Evaluation WorkerSet and metrics last returned by `self.evaluate()`.
        self.evaluation_workers: Optional[WorkerSet] = None
        # Initialize common evaluation_metrics to nan, before they become
        # available. We want to make sure the metrics are always present
        # (although their values may be nan), so that Tune does not complain
        # when we use these as stopping criteria.
        self.evaluation_metrics = {
            "evaluation": {
                "episode_reward_max": np.nan,
                "episode_reward_min": np.nan,
                "episode_reward_mean": np.nan,
            }
        }

        super().__init__(config, logger_creator, remote_checkpoint_dir,
                         sync_function_tpl)

    @ExperimentalAPI
    @classmethod
    def get_default_config(cls) -> TrainerConfigDict:
        return cls._default_config or COMMON_CONFIG

    @override(Trainable)
    def setup(self, config: PartialTrainerConfigDict):

        # Setup our config: Merge the user-supplied config (which could
        # be a partial config dict with the class' default).
        self.config = self.merge_trainer_configs(
            self.get_default_config(), config, self._allow_unknown_configs)

        # Validate the framework settings in config.
        self.validate_framework(self.config)

        # Setup the "env creator" callable.
        env = self._env_id
        if env:
            self.config["env"] = env

            # An already registered env.
            if _global_registry.contains(ENV_CREATOR, env):
                self.env_creator = _global_registry.get(ENV_CREATOR, env)

            # A class path specifier.
            elif "." in env:

                def env_creator_from_classpath(env_context):
                    try:
                        env_obj = from_config(env, env_context)
                    except ValueError:
                        raise EnvError(
                            ERR_MSG_INVALID_ENV_DESCRIPTOR.format(env))
                    return env_obj

                self.env_creator = env_creator_from_classpath
            # Try gym/PyBullet/Vizdoom.
            else:
                self.env_creator = functools.partial(
                    gym_env_creator, env_descriptor=env)
        # No env -> Env creator always returns None.
        else:
            self.env_creator = lambda env_config: None

        # Set Trainer's seed after we have - if necessary - enabled
        # tf eager-execution.
        update_global_seed_if_necessary(self.config["framework"],
                                        self.config["seed"])

        self.validate_config(self.config)
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

        # Create local replay buffer if necessary.
        self.local_replay_buffer = (
            self._create_local_replay_buffer_if_necessary(self.config))

        # Create a dict, mapping ActorHandles to sets of open remote
        # requests (object refs). This way, we keep track, of which actors
        # inside this Trainer (e.g. a remote RolloutWorker) have
        # already been sent how many (e.g. `sample()`) requests.
        self.remote_requests_in_flight: \
            DefaultDict[ActorHandle, Set[ray.ObjectRef]] = defaultdict(set)

        # Deprecated way of implementing Trainer sub-classes (or "templates"
        # via the soon-to-be deprecated `build_trainer` utility function).
        # Instead, sub-classes should override the Trainable's `setup()`
        # method and call super().setup() from within that override at some
        # point.
        self.workers: Optional[WorkerSet] = None
        self.train_exec_impl = None

        # Old design: Override `Trainer._init` (or use `build_trainer()`, which
        # will do this for you).
        try:
            self._init(self.config, self.env_creator)
        # New design: Override `Trainable.setup()` (as indented by Trainable)
        # and do or don't call super().setup() from within your override.
        # By default, `super().setup()` will create both worker sets:
        # "rollout workers" for collecting samples for training and - if
        # applicable - "evaluation workers" for evaluation runs in between or
        # parallel to training.
        # TODO: Deprecate `_init()` and remove this try/except block.
        except NotImplementedError:
            # Only if user did not override `_init()`:
            # - Create rollout workers here automatically.
            # - Run the execution plan to create the local iterator to `next()`
            #   in each training iteration.
            # This matches the behavior of using `build_trainer()`, which
            # should no longer be used.
            self.workers = self._make_workers(
                env_creator=self.env_creator,
                validate_env=self.validate_env,
                policy_class=self.get_default_policy_class(self.config),
                config=self.config,
                num_workers=self.config["num_workers"])

            # Function defining one single training iteration's behavior.
            if self.config["_disable_execution_plan_api"]:
                # Ensure remote workers are initially in sync with the
                # local worker.
                self.workers.sync_weights()
            # LocalIterator-creating "execution plan".
            # Only call this once here to create `self.train_exec_impl`,
            # which is a ray.util.iter.LocalIterator that will be `next`'d
            # on each training iteration.
            else:
                self.train_exec_impl = self.execution_plan(
                    self.workers, self.config,
                    **self._kwargs_for_execution_plan())

            # Now that workers have been created, update our policy
            # specs in the config[multiagent] dict with the correct spaces.
            self.config["multiagent"]["policies"] = \
                self.workers.local_worker().policy_dict

        # Evaluation WorkerSet setup.
        # User would like to setup a separate evaluation worker set.

        # Update with evaluation settings:
        user_eval_config = copy.deepcopy(self.config["evaluation_config"])

        # Assert that user has not unset "in_evaluation".
        assert "in_evaluation" not in user_eval_config or \
               user_eval_config["in_evaluation"] is True

        # Merge user-provided eval config with the base config. This makes sure
        # the eval config is always complete, no matter whether we have eval
        # workers or perform evaluation on the (non-eval) local worker.
        eval_config = merge_dicts(self.config, user_eval_config)
        self.config["evaluation_config"] = eval_config

        if self.config.get("evaluation_num_workers", 0) > 0 or \
                self.config.get("evaluation_interval"):
            logger.debug(f"Using evaluation_config: {user_eval_config}.")

            # Validate evaluation config.
            self.validate_config(eval_config)

            # Set the `in_evaluation` flag.
            eval_config["in_evaluation"] = True

            # Evaluation duration unit: episodes.
            # Switch on `complete_episode` rollouts. Also, make sure
            # rollout fragments are short so we never have more than one
            # episode in one rollout.
            if eval_config["evaluation_duration_unit"] == "episodes":
                eval_config.update({
                    "batch_mode": "complete_episodes",
                    "rollout_fragment_length": 1,
                })
            # Evaluation duration unit: timesteps.
            # - Set `batch_mode=truncate_episodes` so we don't perform rollouts
            #   strictly along episode borders.
            # Set `rollout_fragment_length` such that desired steps are divided
            # equally amongst workers or - in "auto" duration mode - set it
            # to a reasonably small number (10), such that a single `sample()`
            # call doesn't take too much time so we can stop evaluation as soon
            # as possible after the train step is completed.
            else:
                eval_config.update({
                    "batch_mode": "truncate_episodes",
                    "rollout_fragment_length": 10
                    if self.config["evaluation_duration"] == "auto" else int(
                        math.ceil(
                            self.config["evaluation_duration"] /
                            (self.config["evaluation_num_workers"] or 1))),
                })

            self.config["evaluation_config"] = eval_config

            # Create a separate evaluation worker set for evaluation.
            # If evaluation_num_workers=0, use the evaluation set's local
            # worker for evaluation, otherwise, use its remote workers
            # (parallelized evaluation).
            self.evaluation_workers: WorkerSet = self._make_workers(
                env_creator=self.env_creator,
                validate_env=None,
                policy_class=self.get_default_policy_class(self.config),
                config=eval_config,
                num_workers=self.config["evaluation_num_workers"],
                # Don't even create a local worker if num_workers > 0.
                local_worker=False,
            )

    # TODO: Deprecated: In your sub-classes of Trainer, override `setup()`
    #  directly and call super().setup() from within it if you would like the
    #  default setup behavior plus some own setup logic.
    #  If you don't need the env/workers/config/etc.. setup for you by super,
    #  simply do not call super().setup() from your overridden method.
    def _init(self, config: TrainerConfigDict,
              env_creator: Callable[[EnvContext], EnvType]) -> None:
        raise NotImplementedError

    @ExperimentalAPI
    def get_default_policy_class(self, config: TrainerConfigDict) -> \
            Type[Policy]:
        """Returns a default Policy class to use, given a config.

        This class will be used inside RolloutWorkers' PolicyMaps in case
        the policy class is not provided by the user in any single- or
        multi-agent PolicySpec.

        This method is experimental and currently only used, iff the Trainer
        class was not created using the `build_trainer` utility and if
        the Trainer sub-class does not override `_init()` and create it's
        own WorkerSet in `_init()`.
        """
        return getattr(self, "_policy_class", None)

    @override(Trainable)
    def step(self) -> ResultDict:
        """Implements the main `Trainer.train()` logic.

        Takes n attempts to perform a single training step. Thereby
        catches RayErrors resulting from worker failures. After n attempts,
        fails gracefully.

        Override this method in your Trainer sub-classes if you would like to
        handle worker failures yourself. Otherwise, override
        `self.step_attempt()` to keep the n attempts (catch worker failures).

        Returns:
            The results dict with stats/infos on sampling, training,
            and - if required - evaluation.
        """
        step_attempt_results = None

        with self._step_context() as step_ctx:
            while not step_ctx.should_stop(step_attempt_results):
                # Try to train one step.
                try:
                    step_attempt_results = self.step_attempt()
                # @ray.remote RolloutWorker failure.
                except RayError as e:
                    # Try to recover w/o the failed worker.
                    if self.config["ignore_worker_failures"]:
                        logger.exception(
                            "Error in train call, attempting to recover")
                        self.try_recover_from_step_attempt()
                    # Error out.
                    else:
                        logger.warning(
                            "Worker crashed during call to `step_attempt()`. "
                            "To try to continue training without the failed "
                            "worker, set `ignore_worker_failures=True`.")
                        raise e
                # Any other exception.
                except Exception as e:
                    # Allow logs messages to propagate.
                    time.sleep(0.5)
                    raise e

        result = step_attempt_results

        if hasattr(self, "workers") and isinstance(self.workers, WorkerSet):
            # Sync filters on workers.
            self._sync_filters_if_needed(self.workers)

            # Collect worker metrics.
            if self.config["_disable_execution_plan_api"]:
                result = self._compile_step_results(
                    step_ctx=step_ctx,
                    step_attempt_results=step_attempt_results,
                )

        return result

    @ExperimentalAPI
    def step_attempt(self) -> ResultDict:
        """Attempts a single training step, including evaluation, if required.

        Override this method in your Trainer sub-classes if you would like to
        keep the n step-attempts logic (catch worker failures) in place or
        override `step()` directly if you would like to handle worker
        failures yourself.

        Returns:
            The results dict with stats/infos on sampling, training,
            and - if required - evaluation.
        """

        def auto_duration_fn(unit, num_eval_workers, eval_cfg, num_units_done):
            # Training is done and we already ran at least one
            # evaluation -> Nothing left to run.
            if num_units_done > 0 and \
                    train_future.done():
                return 0
            # Count by episodes. -> Run n more
            # (n=num eval workers).
            elif unit == "episodes":
                return num_eval_workers
            # Count by timesteps. -> Run n*m*p more
            # (n=num eval workers; m=rollout fragment length;
            # p=num-envs-per-worker).
            else:
                return num_eval_workers * \
                       eval_cfg["rollout_fragment_length"] * \
                       eval_cfg["num_envs_per_worker"]

        # self._iteration gets incremented after this function returns,
        # meaning that e. g. the first time this function is called,
        # self._iteration will be 0.
        evaluate_this_iter = \
            self.config["evaluation_interval"] and \
            (self._iteration + 1) % self.config["evaluation_interval"] == 0

        step_results = {}

        # No evaluation necessary, just run the next training iteration.
        if not evaluate_this_iter:
            step_results = self._exec_plan_or_training_iteration_fn()
        # We have to evaluate in this training iteration.
        else:
            # No parallelism.
            if not self.config["evaluation_parallel_to_training"]:
                step_results = self._exec_plan_or_training_iteration_fn()

            # Kick off evaluation-loop (and parallel train() call,
            # if requested).
            # Parallel eval + training.
            if self.config["evaluation_parallel_to_training"]:
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    train_future = executor.submit(
                        lambda: self._exec_plan_or_training_iteration_fn())
                    # Automatically determine duration of the evaluation.
                    if self.config["evaluation_duration"] == "auto":
                        unit = self.config["evaluation_duration_unit"]
                        step_results.update(
                            self.evaluate(
                                duration_fn=functools.partial(
                                    auto_duration_fn, unit, self.config[
                                        "evaluation_num_workers"], self.config[
                                            "evaluation_config"])))
                    else:
                        step_results.update(self.evaluate())
                    # Collect the training results from the future.
                    step_results.update(train_future.result())
            # Sequential: train (already done above), then eval.
            else:
                step_results.update(self.evaluate())

        # Attach latest available evaluation results to train results,
        # if necessary.
        if (not evaluate_this_iter
                and self.config["always_attach_evaluation_results"]):
            assert isinstance(self.evaluation_metrics, dict), \
                "Trainer.evaluate() needs to return a dict."
            step_results.update(self.evaluation_metrics)

        # Check `env_task_fn` for possible update of the env's task.
        if self.config["env_task_fn"] is not None:
            if not callable(self.config["env_task_fn"]):
                raise ValueError(
                    "`env_task_fn` must be None or a callable taking "
                    "[train_results, env, env_ctx] as args!")

            def fn(env, env_context, task_fn):
                new_task = task_fn(step_results, env, env_context)
                cur_task = env.get_task()
                if cur_task != new_task:
                    env.set_task(new_task)

            fn = functools.partial(fn, task_fn=self.config["env_task_fn"])
            self.workers.foreach_env_with_context(fn)

        return step_results

    @PublicAPI
    def evaluate(
            self,
            episodes_left_fn=None,  # deprecated
            duration_fn: Optional[Callable[[int], int]] = None,
    ) -> dict:
        """Evaluates current policy under `evaluation_config` settings.

        Note that this default implementation does not do anything beyond
        merging evaluation_config with the normal trainer config.

        Args:
            duration_fn: An optional callable taking the already run
                num episodes as only arg and returning the number of
                episodes left to run. It's used to find out whether
                evaluation should continue.
        """
        if episodes_left_fn is not None:
            deprecation_warning(
                old="Trainer.evaluate(episodes_left_fn)",
                new="Trainer.evaluate(duration_fn)",
                error=False)
            duration_fn = episodes_left_fn

        # In case we are evaluating (in a thread) parallel to training,
        # we may have to re-enable eager mode here (gets disabled in the
        # thread).
        if self.config.get("framework") in ["tf2", "tfe"] and \
                not tf.executing_eagerly():
            tf1.enable_eager_execution()

        # Call the `_before_evaluate` hook.
        self._before_evaluate()

        # Sync weights to the evaluation WorkerSet.
        if self.evaluation_workers is not None:
            self.evaluation_workers.sync_weights(
                from_worker=self.workers.local_worker())
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
            if self.evaluation_workers is None and \
                    self.workers.local_worker().input_reader is None:
                raise ValueError(
                    "Cannot evaluate w/o an evaluation worker set in "
                    "the Trainer or w/o an env on the local worker!\n"
                    "Try one of the following:\n1) Set "
                    "`evaluation_interval` >= 0 to force creating a "
                    "separate evaluation worker set.\n2) Set "
                    "`create_env_on_driver=True` to force the local "
                    "(non-eval) worker to have an environment to "
                    "evaluate on.")

            # How many episodes/timesteps do we need to run?
            # In "auto" mode (only for parallel eval + training): Run as long
            # as training lasts.
            unit = self.config["evaluation_duration_unit"]
            eval_cfg = self.config["evaluation_config"]
            rollout = eval_cfg["rollout_fragment_length"]
            num_envs = eval_cfg["num_envs_per_worker"]
            duration = self.config["evaluation_duration"] if \
                self.config["evaluation_duration"] != "auto" else \
                (self.config["evaluation_num_workers"] or 1) * \
                (1 if unit == "episodes" else rollout)
            num_ts_run = 0

            # Default done-function returns True, whenever num episodes
            # have been completed.
            if duration_fn is None:

                def duration_fn(num_units_done):
                    return duration - num_units_done

            logger.info(f"Evaluating current policy for {duration} {unit}.")

            metrics = None
            # No evaluation worker set ->
            # Do evaluation using the local worker. Expect error due to the
            # local worker not having an env.
            if self.evaluation_workers is None:
                # If unit=episodes -> Run n times `sample()` (each sample
                # produces exactly 1 episode).
                # If unit=ts -> Run 1 `sample()` b/c the
                # `rollout_fragment_length` is exactly the desired ts.
                iters = duration if unit == "episodes" else 1
                for _ in range(iters):
                    num_ts_run += len(self.workers.local_worker().sample())
                metrics = collect_metrics(self.workers.local_worker())

            # Evaluation worker set only has local worker.
            elif self.config["evaluation_num_workers"] == 0:
                # If unit=episodes -> Run n times `sample()` (each sample
                # produces exactly 1 episode).
                # If unit=ts -> Run 1 `sample()` b/c the
                # `rollout_fragment_length` is exactly the desired ts.
                iters = duration if unit == "episodes" else 1
                for _ in range(iters):
                    num_ts_run += len(
                        self.evaluation_workers.local_worker().sample())

            # Evaluation worker set has n remote workers.
            else:
                # How many episodes have we run (across all eval workers)?
                num_units_done = 0
                round_ = 0
                while True:
                    units_left_to_do = duration_fn(num_units_done)
                    if units_left_to_do <= 0:
                        break

                    round_ += 1
                    batches = ray.get([
                        w.sample.remote() for i, w in enumerate(
                            self.evaluation_workers.remote_workers())
                        if i * (1 if unit == "episodes" else rollout *
                                num_envs) < units_left_to_do
                    ])
                    # 1 episode per returned batch.
                    if unit == "episodes":
                        num_units_done += len(batches)
                    # n timesteps per returned batch.
                    else:
                        ts = sum(len(b) for b in batches)
                        num_ts_run += ts
                        num_units_done += ts

                    logger.info(f"Ran round {round_} of parallel evaluation "
                                f"({num_units_done}/{duration} {unit} done)")

            if metrics is None:
                metrics = collect_metrics(
                    self.evaluation_workers.local_worker(),
                    self.evaluation_workers.remote_workers())
            metrics["timesteps_this_iter"] = num_ts_run

        # Evaluation does not run for every step.
        # Save evaluation metrics on trainer, so it can be attached to
        # subsequent step results as latest evaluation result.
        self.evaluation_metrics = {"evaluation": metrics}

        # Also return the results here for convenience.
        return self.evaluation_metrics

    @ExperimentalAPI
    def training_iteration(self) -> ResultDict:
        """Default single iteration logic of an algorithm.

        - Collect on-policy samples (SampleBatches) in parallel using the
          Trainer's RolloutWorkers (@ray.remote).
        - Concatenate collected SampleBatches into one train batch.
        - Note that we may have more than one policy in the multi-agent case:
          Call the different policies' `learn_on_batch` (simple optimizer) OR
          `load_batch_into_buffer` + `learn_on_loaded_batch` (multi-GPU
          optimizer) methods to calculate loss and update the model(s).
        - Return all collected metrics for the iteration.

        Returns:
            The results dict from executing the training iteration.
        """
        # Some shortcuts.
        batch_size = self.config["train_batch_size"]

        # Collects SampleBatches in parallel and synchronously
        # from the Trainer's RolloutWorkers until we hit the
        # configured `train_batch_size`.
        sample_batches = []
        num_env_steps = 0
        num_agent_steps = 0
        while (not self._by_agent_steps and num_env_steps < batch_size) or \
                (self._by_agent_steps and num_agent_steps < batch_size):
            new_sample_batches = synchronous_parallel_sample(self.workers)
            sample_batches.extend(new_sample_batches)
            num_env_steps += sum(len(s) for s in new_sample_batches)
            num_agent_steps += sum(
                len(s) if isinstance(s, SampleBatch) else s.agent_steps()
                for s in new_sample_batches)
        self._counters[NUM_ENV_STEPS_SAMPLED] += num_env_steps
        self._counters[NUM_AGENT_STEPS_SAMPLED] += num_agent_steps

        # Combine all batches at once
        train_batch = SampleBatch.concat_samples(sample_batches)

        # Use simple optimizer (only for multi-agent or tf-eager; all other
        # cases should use the multi-GPU optimizer, even if only using 1 GPU).
        # TODO: (sven) rename MultiGPUOptimizer into something more
        #  meaningful.
        if self.config.get("simple_optimizer") is True:
            train_results = train_one_step(self, train_batch)
        else:
            train_results = multi_gpu_train_one_step(self, train_batch)

        # Update weights - after learning on the local worker - on all remote
        # workers.
        if self.workers.remote_workers():
            with self._timers[WORKER_UPDATE_TIMER]:
                self.workers.sync_weights()

        return train_results

    @DeveloperAPI
    @staticmethod
    def execution_plan(workers, config, **kwargs):

        # Collects experiences in parallel from multiple RolloutWorker actors.
        rollouts = ParallelRollouts(workers, mode="bulk_sync")

        # Combine experiences batches until we hit `train_batch_size` in size.
        # Then, train the policy on those experiences and update the workers.
        train_op = rollouts.combine(
            ConcatBatches(
                min_batch_size=config["train_batch_size"],
                count_steps_by=config["multiagent"]["count_steps_by"],
            ))

        if config.get("simple_optimizer") is True:
            train_op = train_op.for_each(TrainOneStep(workers))
        else:
            train_op = train_op.for_each(
                MultiGPUTrainOneStep(
                    workers=workers,
                    sgd_minibatch_size=config.get("sgd_minibatch_size",
                                                  config["train_batch_size"]),
                    num_sgd_iter=config.get("num_sgd_iter", 1),
                    num_gpus=config["num_gpus"],
                    _fake_gpus=config["_fake_gpus"]))

        # Add on the standard episode reward, etc. metrics reporting. This
        # returns a LocalIterator[metrics_dict] representing metrics for each
        # train step.
        return StandardMetricsReporting(train_op, workers, config)

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

            # Deprecated args.
            unsquash_actions=DEPRECATED_VALUE,
            clip_actions=DEPRECATED_VALUE,

            # Kwargs placeholder for future compatibility.
            **kwargs,
    ) -> Union[TensorStructType, Tuple[TensorStructType, List[TensorType],
                                       Dict[str, TensorType]]]:
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
                Default: None -> use self.config["explore"].
            timestep: The current (sampling) time step.
            episode: This provides access to all of the internal episodes'
                state, which may be useful for model-based or multi-agent
                algorithms.
            unsquash_action: Should actions be unsquashed according to the
                env's/Policy's action space? If None, use the value of
                self.config["normalize_actions"].
            clip_action: Should actions be clipped according to the
                env's/Policy's action space? If None, use the value of
                self.config["clip_actions"].

        Keyword Args:
            kwargs: forward compatibility placeholder

        Returns:
            The computed action if full_fetch=False, or a tuple of a) the
            full output of policy.compute_actions() if full_fetch=True
            or we have an RNN-based Policy.

        Raises:
            KeyError: If the `policy_id` cannot be found in this Trainer's
                local worker.
        """
        if clip_actions != DEPRECATED_VALUE:
            deprecation_warning(
                old="Trainer.compute_single_action(`clip_actions`=...)",
                new="Trainer.compute_single_action(`clip_action`=...)",
                error=False)
            clip_action = clip_actions
        if unsquash_actions != DEPRECATED_VALUE:
            deprecation_warning(
                old="Trainer.compute_single_action(`unsquash_actions`=...)",
                new="Trainer.compute_single_action(`unsquash_action`=...)",
                error=False)
            unsquash_action = unsquash_actions

        # `unsquash_action` is None: Use value of config['normalize_actions'].
        if unsquash_action is None:
            unsquash_action = self.config["normalize_actions"]
        # `clip_action` is None: Use value of config['clip_actions'].
        elif clip_action is None:
            clip_action = self.config["clip_actions"]

        # User provided an input-dict: Assert that `obs`, `prev_a|r`, `state`
        # are all None.
        err_msg = "Provide either `input_dict` OR [`observation`, ...] as " \
                  "args to Trainer.compute_single_action!"
        if input_dict is not None:
            assert observation is None and prev_action is None and \
                   prev_reward is None and state is None, err_msg
            observation = input_dict[SampleBatch.OBS]
        else:
            assert observation is not None, err_msg

        # Get the policy to compute the action for (in the multi-agent case,
        # Trainer may hold >1 policies).
        policy = self.get_policy(policy_id)
        if policy is None:
            raise KeyError(
                f"PolicyID '{policy_id}' not found in PolicyMap of the "
                f"Trainer's local worker!")
        local_worker = self.workers.local_worker()

        # Check the preprocessor and preprocess, if necessary.
        pp = local_worker.preprocessors[policy_id]
        if pp and type(pp).__name__ != "NoPreprocessor":
            observation = pp.transform(observation)
        observation = local_worker.filters[policy_id](
            observation, update=False)

        # Input-dict.
        if input_dict is not None:
            input_dict[SampleBatch.OBS] = observation
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
            action = space_utils.unsquash_action(action,
                                                 policy.action_space_struct)
        # Clip, according to env's action space.
        elif clip_action:
            action = space_utils.clip_action(action,
                                             policy.action_space_struct)

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
            # Deprecated.
            normalize_actions=None,
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
                action (default: None -> use self.config["explore"]).
            timestep: The current (sampling) time step.
            episodes: This provides access to all of the internal episodes'
                state, which may be useful for model-based or multi-agent
                algorithms.
            unsquash_actions: Should actions be unsquashed according
                to the env's/Policy's action space? If None, use
                self.config["normalize_actions"].
            clip_actions: Should actions be clipped according to the
                env's/Policy's action space? If None, use
                self.config["clip_actions"].

        Keyword Args:
            kwargs: forward compatibility placeholder

        Returns:
            The computed action if full_fetch=False, or a tuple consisting of
            the full output of policy.compute_actions_from_input_dict() if
            full_fetch=True or we have an RNN-based Policy.
        """
        if normalize_actions is not None:
            deprecation_warning(
                old="Trainer.compute_actions(`normalize_actions`=...)",
                new="Trainer.compute_actions(`unsquash_actions`=...)",
                error=False)
            unsquash_actions = normalize_actions

        # `unsquash_actions` is None: Use value of config['normalize_actions'].
        if unsquash_actions is None:
            unsquash_actions = self.config["normalize_actions"]
        # `clip_actions` is None: Use value of config['clip_actions'].
        elif clip_actions is None:
            clip_actions = self.config["clip_actions"]

        # Preprocess obs and states.
        state_defined = state is not None
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

        input_dict = {SampleBatch.OBS: obs_batch}
        if prev_action:
            input_dict[SampleBatch.PREV_ACTIONS] = prev_action
        if prev_reward:
            input_dict[SampleBatch.PREV_REWARDS] = prev_reward
        if info:
            input_dict[SampleBatch.INFOS] = info
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
            policy_cls: Type[Policy],
            *,
            observation_space: Optional[gym.spaces.Space] = None,
            action_space: Optional[gym.spaces.Space] = None,
            config: Optional[PartialTrainerConfigDict] = None,
            policy_mapping_fn: Optional[Callable[[AgentID, EpisodeID],
                                                 PolicyID]] = None,
            policies_to_train: Optional[List[PolicyID]] = None,
            evaluation_workers: bool = True,
    ) -> Policy:
        """Adds a new policy to this Trainer.

        Args:
            policy_id (PolicyID): ID of the policy to add.
            policy_cls (Type[Policy]): The Policy class to use for
                constructing the new Policy.
            observation_space (Optional[gym.spaces.Space]): The observation
                space of the policy to add.
            action_space (Optional[gym.spaces.Space]): The action space
                of the policy to add.
            config (Optional[PartialTrainerConfigDict]): The config overrides
                for the policy to add.
            policy_mapping_fn (Optional[Callable[[AgentID], PolicyID]]): An
                optional (updated) policy mapping function to use from here on.
                Note that already ongoing episodes will not change their
                mapping but will use the old mapping till the end of the
                episode.
            policies_to_train (Optional[List[PolicyID]]): An optional list of
                policy IDs to be trained. If None, will keep the existing list
                in place. Policies, whose IDs are not in the list will not be
                updated.
            evaluation_workers (bool): Whether to add the new policy also
                to the evaluation WorkerSet.

        Returns:
            The newly added policy (the copy that got added to the local
            worker).
        """

        def fn(worker: RolloutWorker):
            # `foreach_worker` function: Adds the policy the the worker (and
            # maybe changes its policy_mapping_fn - if provided here).
            worker.add_policy(
                policy_id=policy_id,
                policy_cls=policy_cls,
                observation_space=observation_space,
                action_space=action_space,
                config=config,
                policy_mapping_fn=policy_mapping_fn,
                policies_to_train=policies_to_train,
            )

        # Run foreach_worker fn on all workers (incl. evaluation workers).
        self.workers.foreach_worker(fn)
        if evaluation_workers and self.evaluation_workers is not None:
            self.evaluation_workers.foreach_worker(fn)

        # Return newly added policy (from the local rollout worker).
        return self.get_policy(policy_id)

    @PublicAPI
    def remove_policy(
            self,
            policy_id: PolicyID = DEFAULT_POLICY_ID,
            *,
            policy_mapping_fn: Optional[Callable[[AgentID], PolicyID]] = None,
            policies_to_train: Optional[List[PolicyID]] = None,
            evaluation_workers: bool = True,
    ) -> None:
        """Removes a new policy from this Trainer.

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
            evaluation_workers (bool): Whether to also remove the policy from
                the evaluation WorkerSet.
        """

        def fn(worker):
            worker.remove_policy(
                policy_id=policy_id,
                policy_mapping_fn=policy_mapping_fn,
                policies_to_train=policies_to_train,
            )

        self.workers.foreach_worker(fn)
        if evaluation_workers and self.evaluation_workers is not None:
            self.evaluation_workers.foreach_worker(fn)

    @DeveloperAPI
    def export_policy_model(self,
                            export_dir: str,
                            policy_id: PolicyID = DEFAULT_POLICY_ID,
                            onnx: Optional[int] = None) -> None:
        """Exports policy model with given policy_id to a local directory.

        Args:
            export_dir: Writable local directory.
            policy_id: Optional policy id to export.
            onnx: If given, will export model in ONNX format. The
                value of this parameter set the ONNX OpSet version to use.
                If None, the output format will be DL framework specific.

        Example:
            >>> trainer = MyTrainer()
            >>> for _ in range(10):
            >>>     trainer.train()
            >>> trainer.export_policy_model("/tmp/dir")
            >>> trainer.export_policy_model("/tmp/dir/onnx", onnx=1)
        """
        self.get_policy(policy_id).export_model(export_dir, onnx)

    @DeveloperAPI
    def export_policy_checkpoint(
            self,
            export_dir: str,
            filename_prefix: str = "model",
            policy_id: PolicyID = DEFAULT_POLICY_ID,
    ) -> None:
        """Exports policy model checkpoint to a local directory.

        Args:
            export_dir: Writable local directory.
            filename_prefix: file name prefix of checkpoint files.
            policy_id: Optional policy id to export.

        Example:
            >>> trainer = MyTrainer()
            >>> for _ in range(10):
            >>>     trainer.train()
            >>> trainer.export_policy_checkpoint("/tmp/export_dir")
        """
        self.get_policy(policy_id).export_checkpoint(export_dir,
                                                     filename_prefix)

    @DeveloperAPI
    def import_policy_model_from_h5(
            self,
            import_file: str,
            policy_id: PolicyID = DEFAULT_POLICY_ID,
    ) -> None:
        """Imports a policy's model with given policy_id from a local h5 file.

        Args:
            import_file: The h5 file to import from.
            policy_id: Optional policy id to import into.

        Example:
            >>> trainer = MyTrainer()
            >>> trainer.import_policy_model_from_h5("/tmp/weights.h5")
            >>> for _ in range(10):
            >>>     trainer.train()
        """
        self.get_policy(policy_id).import_model_from_h5(import_file)
        # Sync new weights to remote workers.
        self._sync_weights_to_workers(worker_set=self.workers)

    @override(Trainable)
    def save_checkpoint(self, checkpoint_dir: str) -> str:
        checkpoint_path = os.path.join(checkpoint_dir,
                                       "checkpoint-{}".format(self.iteration))
        pickle.dump(self.__getstate__(), open(checkpoint_path, "wb"))

        return checkpoint_path

    @override(Trainable)
    def load_checkpoint(self, checkpoint_path: str) -> None:
        extra_data = pickle.load(open(checkpoint_path, "rb"))
        self.__setstate__(extra_data)

    @override(Trainable)
    def log_result(self, result: ResultDict) -> None:
        # Log after the callback is invoked, so that the user has a chance
        # to mutate the result.
        self.callbacks.on_train_result(trainer=self, result=result)
        # Then log according to Trainable's logging logic.
        Trainable.log_result(self, result)

    @override(Trainable)
    def cleanup(self) -> None:
        # Stop all workers.
        workers = getattr(self, "workers", None)
        if workers:
            workers.stop()
        # Stop all optimizers.
        if hasattr(self, "optimizer") and self.optimizer:
            self.optimizer.stop()

    @classmethod
    @override(Trainable)
    def default_resource_request(
            cls, config: PartialTrainerConfigDict) -> \
            Union[Resources, PlacementGroupFactory]:

        # Default logic for RLlib algorithms (Trainers):
        # Create one bundle per individual worker (local or remote).
        # Use `num_cpus_for_driver` and `num_gpus` for the local worker and
        # `num_cpus_per_worker` and `num_gpus_per_worker` for the remote
        # workers to determine their CPU/GPU resource needs.

        # Convenience config handles.
        cf = dict(cls.get_default_config(), **config)
        eval_cf = cf["evaluation_config"]

        # TODO(ekl): add custom resources here once tune supports them
        # Return PlacementGroupFactory containing all needed resources
        # (already properly defined as device bundles).
        return PlacementGroupFactory(
            bundles=[{
                # Local worker.
                "CPU": cf["num_cpus_for_driver"],
                "GPU": 0 if cf["_fake_gpus"] else cf["num_gpus"],
            }] + [
                {
                    # RolloutWorkers.
                    "CPU": cf["num_cpus_per_worker"],
                    "GPU": cf["num_gpus_per_worker"],
                } for _ in range(cf["num_workers"])
            ] + ([
                {
                    # Evaluation workers.
                    # Note: The local eval worker is located on the driver CPU.
                    "CPU": eval_cf.get("num_cpus_per_worker",
                                       cf["num_cpus_per_worker"]),
                    "GPU": eval_cf.get("num_gpus_per_worker",
                                       cf["num_gpus_per_worker"]),
                } for _ in range(cf["evaluation_num_workers"])
            ] if cf["evaluation_interval"] else []),
            strategy=config.get("placement_strategy", "PACK"))

    @DeveloperAPI
    def _before_evaluate(self):
        """Pre-evaluation callback."""
        pass

    @DeveloperAPI
    def _make_workers(
            self,
            *,
            env_creator: Callable[[EnvContext], EnvType],
            validate_env: Optional[Callable[[EnvType, EnvContext], None]],
            policy_class: Type[Policy],
            config: TrainerConfigDict,
            num_workers: int,
            local_worker: bool = True,
    ) -> WorkerSet:
        """Default factory method for a WorkerSet running under this Trainer.

        Override this method by passing a custom `make_workers` into
        `build_trainer`.

        Args:
            env_creator: A function that return and Env given an env
                config.
            validate_env: Optional callable to validate the generated
                environment. The env to be checked is the one returned from
                the env creator, which may be a (single, not-yet-vectorized)
                gym.Env or your custom RLlib env type (e.g. MultiAgentEnv,
                VectorEnv, BaseEnv, etc..).
            policy_class: The Policy class to use for creating the policies
                of the workers.
            config: The Trainer's config.
            num_workers: Number of remote rollout workers to create.
                0 for local only.
            local_worker: Whether to create a local (non @ray.remote) worker
                in the returned set as well (default: True). If `num_workers`
                is 0, always create a local worker.

        Returns:
            The created WorkerSet.
        """
        return WorkerSet(
            env_creator=env_creator,
            validate_env=validate_env,
            policy_class=policy_class,
            trainer_config=config,
            num_workers=num_workers,
            local_worker=local_worker,
            logdir=self.logdir,
        )

    def _sync_filters_if_needed(self, workers: WorkerSet):
        if self.config.get("observation_filter", "NoFilter") != "NoFilter":
            FilterManager.synchronize(
                workers.local_worker().filters,
                workers.remote_workers(),
                update_remote=self.config["synchronize_filters"])
            logger.debug("synchronized filters: {}".format(
                workers.local_worker().filters))

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
        logger.info("Synchronizing weights to workers.")
        weights = ray.put(self.workers.local_worker().save())
        worker_set.foreach_worker(lambda w: w.restore(ray.get(weights)))

    def _exec_plan_or_training_iteration_fn(self):
        if self.config["_disable_execution_plan_api"]:
            results = self.training_iteration()
        else:
            results = next(self.train_exec_impl)
        return results

    @classmethod
    @override(Trainable)
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
        """Merges a complete Trainer config with a partial override dict.

        Respects nested structures within the config dicts. The values in the
        partial override dict take priority.

        Args:
            config1: The complete Trainer's dict to be merged (overridden)
                with `config2`.
            config2: The partial override config dict to merge on top of
                `config1`.
            _allow_unknown_configs: If True, keys in `config2` that don't exist
                in `config1` are allowed and will be added to the final config.

        Returns:
            The merged full trainer config dict.
        """
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
    def validate_framework(config: PartialTrainerConfigDict) -> None:
        """Validates the config dictionary wrt the framework settings.

        Args:
            config: The config dictionary to be validated.

        """
        _tf1, _tf, _tfv = None, None, None
        _torch = None
        framework = config["framework"]
        tf_valid_frameworks = {"tf", "tf2", "tfe"}
        if framework not in tf_valid_frameworks and framework != "torch":
            return
        elif framework in tf_valid_frameworks:
            _tf1, _tf, _tfv = try_import_tf()
        else:
            _torch, _ = try_import_torch()

        def check_if_correct_nn_framework_installed():
            """Check if tf/torch experiment is running and tf/torch installed.
            """
            if framework in tf_valid_frameworks:
                if not (_tf1 or _tf):
                    raise ImportError((
                        "TensorFlow was specified as the 'framework' "
                        "inside of your config dictionary. However, there was "
                        "no installation found. You can install TensorFlow "
                        "via `pip install tensorflow`"))
            elif framework == "torch":
                if not _torch:
                    raise ImportError(
                        ("PyTorch was specified as the 'framework' inside "
                         "of your config dictionary. However, there was no "
                         "installation found. You can install PyTorch via "
                         "`pip install torch`"))

        def resolve_tf_settings():
            """Check and resolve tf settings."""

            if _tf1 and config["framework"] in ["tf2", "tfe"]:
                if config["framework"] == "tf2" and _tfv < 2:
                    raise ValueError(
                        "You configured `framework`=tf2, but your installed "
                        "pip tf-version is < 2.0! Make sure your TensorFlow "
                        "version is >= 2.x.")
                if not _tf1.executing_eagerly():
                    _tf1.enable_eager_execution()
                # Recommend setting tracing to True for speedups.
                logger.info(
                    f"Executing eagerly (framework='{config['framework']}'),"
                    f" with eager_tracing={config['eager_tracing']}. For "
                    "production workloads, make sure to set eager_tracing=True"
                    "  in order to match the speed of tf-static-graph "
                    "(framework='tf'). For debugging purposes, "
                    "`eager_tracing=False` is the best choice.")
            # Tf-static-graph (framework=tf): Recommend upgrading to tf2 and
            # enabling eager tracing for similar speed.
            elif _tf1 and config["framework"] == "tf":
                logger.info(
                    "Your framework setting is 'tf', meaning you are using "
                    "static-graph mode. Set framework='tf2' to enable eager "
                    "execution with tf2.x. You may also then want to set "
                    "eager_tracing=True in order to reach similar execution "
                    "speed as with static-graph mode.")

        check_if_correct_nn_framework_installed()
        resolve_tf_settings()

    @ExperimentalAPI
    def validate_config(self, config: TrainerConfigDict) -> None:
        """Validates a given config dict for this Trainer.

        Users should override this method to implement custom validation
        behavior. It is recommended to call `super().validate_config()` in
        this override.

        Args:
            config: The given config dict to check.

        Raises:
            ValueError: If there is something wrong with the config.
        """
        model_config = config.get("model")
        if model_config is None:
            config["model"] = model_config = {}

        # Monitor should be replaced by `record_env`.
        if config.get("monitor", DEPRECATED_VALUE) != DEPRECATED_VALUE:
            deprecation_warning("monitor", "record_env", error=False)
            config["record_env"] = config.get("monitor", False)
        # Empty string would fail some if-blocks checking for this setting.
        # Set to True instead, meaning: use default output dir to store
        # the videos.
        if config.get("record_env") == "":
            config["record_env"] = True

        # DefaultCallbacks if callbacks - for whatever reason - set to
        # None.
        if config["callbacks"] is None:
            config["callbacks"] = DefaultCallbacks

        # Multi-GPU settings.
        simple_optim_setting = config.get("simple_optimizer", DEPRECATED_VALUE)
        if simple_optim_setting != DEPRECATED_VALUE:
            deprecation_warning(old="simple_optimizer", error=False)

        # Validate "multiagent" sub-dict and convert policy 4-tuples to
        # PolicySpec objects.
        policies, is_multi_agent = check_multi_agent(config)

        framework = config.get("framework")
        # Multi-GPU setting: Must use MultiGPUTrainOneStep.
        if config.get("num_gpus", 0) > 1:
            if framework in ["tfe", "tf2"]:
                raise ValueError("`num_gpus` > 1 not supported yet for "
                                 "framework={}!".format(framework))
            elif simple_optim_setting is True:
                raise ValueError(
                    "Cannot use `simple_optimizer` if `num_gpus` > 1! "
                    "Consider not setting `simple_optimizer` in your config.")
            config["simple_optimizer"] = False
        # Auto-setting: Use simple-optimizer for tf-eager or multiagent,
        # otherwise: MultiGPUTrainOneStep (if supported by the algo's execution
        # plan).
        elif simple_optim_setting == DEPRECATED_VALUE:
            # tf-eager: Must use simple optimizer.
            if framework not in ["tf", "torch"]:
                config["simple_optimizer"] = True
            # Multi-agent case: Try using MultiGPU optimizer (only
            # if all policies used are DynamicTFPolicies or TorchPolicies).
            elif is_multi_agent:
                from ray.rllib.policy.dynamic_tf_policy import DynamicTFPolicy
                from ray.rllib.policy.torch_policy import TorchPolicy
                default_policy_cls = self.get_default_policy_class(config)
                if any((p[0] or default_policy_cls) is None
                       or not issubclass(p[0] or default_policy_cls,
                                         (DynamicTFPolicy, TorchPolicy))
                       for p in config["multiagent"]["policies"].values()):
                    config["simple_optimizer"] = True
                else:
                    config["simple_optimizer"] = False
            else:
                config["simple_optimizer"] = False

        # User manually set simple-optimizer to False -> Error if tf-eager.
        elif simple_optim_setting is False:
            if framework in ["tfe", "tf2"]:
                raise ValueError("`simple_optimizer=False` not supported for "
                                 "framework={}!".format(framework))

        # Offline RL settings.
        if isinstance(config["input_evaluation"], tuple):
            config["input_evaluation"] = list(config["input_evaluation"])
        elif not isinstance(config["input_evaluation"], list):
            raise ValueError(
                "`input_evaluation` must be a list of strings, got {}!".format(
                    config["input_evaluation"]))

        # Check model config.
        # If no preprocessing, propagate into model's config as well
        # (so model will know, whether inputs are preprocessed or not).
        if config["_disable_preprocessor_api"] is True:
            model_config["_disable_preprocessor_api"] = True
        # If no action flattening, propagate into model's config as well
        # (so model will know, whether action inputs are already flattened or
        # not).
        if config["_disable_action_flattening"] is True:
            model_config["_disable_action_flattening"] = True

        # Prev_a/r settings.
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

        # Check multi-agent batch count mode.
        if config["multiagent"].get("count_steps_by", "env_steps") not in \
                ["env_steps", "agent_steps"]:
            raise ValueError(
                "`count_steps_by` must be one of [env_steps|agent_steps]! "
                "Got {}".format(config["multiagent"]["count_steps_by"]))
        self._by_agent_steps = self.config["multiagent"].get(
            "count_steps_by") == "agent_steps"

        # Metrics settings.
        if config["metrics_smoothing_episodes"] != DEPRECATED_VALUE:
            deprecation_warning(
                old="metrics_smoothing_episodes",
                new="metrics_num_episodes_for_smoothing",
                error=False,
            )
            config["metrics_num_episodes_for_smoothing"] = \
                config["metrics_smoothing_episodes"]
        if config["min_iter_time_s"] != DEPRECATED_VALUE:
            # TODO: Warn once all algos use the `training_iteration` method.
            # deprecation_warning(
            #     old="min_iter_time_s",
            #     new="min_time_s_per_reporting",
            #     error=False,
            # )
            config["min_time_s_per_reporting"] = \
                config["min_iter_time_s"]

        if config["collect_metrics_timeout"] != DEPRECATED_VALUE:
            # TODO: Warn once all algos use the `training_iteration` method.
            # deprecation_warning(
            #     old="collect_metrics_timeout",
            #     new="metrics_episode_collection_timeout_s",
            #     error=False,
            # )
            config["metrics_episode_collection_timeout_s"] = \
                config["collect_metrics_timeout"]

        if config["timesteps_per_iteration"] != DEPRECATED_VALUE:
            # TODO: Warn once all algos use the `training_iteration` method.
            # deprecation_warning(
            #     old="timesteps_per_iteration",
            #     new="min_sample_timesteps_per_reporting",
            #     error=False,
            # )
            config["min_sample_timesteps_per_reporting"] = \
                config["timesteps_per_iteration"]

        # Metrics settings.
        if config["metrics_smoothing_episodes"] != DEPRECATED_VALUE:
            deprecation_warning(
                old="metrics_smoothing_episodes",
                new="metrics_num_episodes_for_smoothing",
                error=False,
            )
            config["metrics_num_episodes_for_smoothing"] = \
                config["metrics_smoothing_episodes"]

        # Evaluation settings.

        # Deprecated setting: `evaluation_num_episodes`.
        if config["evaluation_num_episodes"] != DEPRECATED_VALUE:
            deprecation_warning(
                old="evaluation_num_episodes",
                new="`evaluation_duration` and `evaluation_duration_unit="
                "episodes`",
                error=False)
            config["evaluation_duration"] = config["evaluation_num_episodes"]
            config["evaluation_duration_unit"] = "episodes"
            config["evaluation_num_episodes"] = DEPRECATED_VALUE

        # If `evaluation_num_workers` > 0, warn if `evaluation_interval` is
        # None (also set `evaluation_interval` to 1).
        if config["evaluation_num_workers"] > 0 and \
                not config["evaluation_interval"]:
            logger.warning(
                f"You have specified {config['evaluation_num_workers']} "
                "evaluation workers, but your `evaluation_interval` is None! "
                "Therefore, evaluation will not occur automatically with each"
                " call to `Trainer.train()`. Instead, you will have to call "
                "`Trainer.evaluate()` manually in order to trigger an "
                "evaluation run.")
        # If `evaluation_num_workers=0` and
        # `evaluation_parallel_to_training=True`, warn that you need
        # at least one remote eval worker for parallel training and
        # evaluation, and set `evaluation_parallel_to_training` to False.
        elif config["evaluation_num_workers"] == 0 and \
                config.get("evaluation_parallel_to_training", False):
            logger.warning(
                "`evaluation_parallel_to_training` can only be done if "
                "`evaluation_num_workers` > 0! Setting "
                "`evaluation_parallel_to_training` to False.")
            config["evaluation_parallel_to_training"] = False

        # If `evaluation_duration=auto`, error if
        # `evaluation_parallel_to_training=False`.
        if config["evaluation_duration"] == "auto":
            if not config["evaluation_parallel_to_training"]:
                raise ValueError(
                    "`evaluation_duration=auto` not supported for "
                    "`evaluation_parallel_to_training=False`!")
        # Make sure, it's an int otherwise.
        elif not isinstance(config["evaluation_duration"], int) or \
                config["evaluation_duration"] <= 0:
            raise ValueError("`evaluation_duration` ({}) must be an int and "
                             ">0!".format(config["evaluation_duration"]))

    @ExperimentalAPI
    @staticmethod
    def validate_env(env: EnvType, env_context: EnvContext) -> None:
        """Env validator function for this Trainer class.

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

    def try_recover_from_step_attempt(self) -> None:
        """Try to identify and remove any unhealthy workers.

        This method is called after an unexpected remote error is encountered
        from a worker during the call to `self.step_attempt()` (within
        `self.step()`). It issues check requests to all current workers and
        removes any that respond with error. If no healthy workers remain,
        an error is raised. Otherwise, tries to re-build the execution plan
        with the remaining (healthy) workers.
        """

        workers = getattr(self, "workers", None)
        if not isinstance(workers, WorkerSet):
            return

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

        logger.warning("Recreating execution plan after failure.")
        workers.reset(healthy_workers)
        if not self.config.get("_disable_execution_plan_api") and \
                callable(self.execution_plan):
            logger.warning("Recreating execution plan after failure")
            self.train_exec_impl = self.execution_plan(
                workers, self.config, **self._kwargs_for_execution_plan())

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
        if ExportFormat.ONNX in export_formats:
            path = os.path.join(export_dir, ExportFormat.ONNX)
            self.export_policy_model(
                path, onnx=int(os.getenv("ONNX_OPSET", "11")))
            exported[ExportFormat.ONNX] = path
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
        # TODO: Experimental functionality: Store contents of replay buffer
        #  to checkpoint, only if user has configured this.
        if self.local_replay_buffer is not None and \
                self.config.get("store_buffer_in_checkpoints"):
            state["local_replay_buffer"] = \
                self.local_replay_buffer.get_state()

        if self.train_exec_impl is not None:
            state["train_exec_impl"] = (
                self.train_exec_impl.shared_metrics.get().save())

        return state

    def __setstate__(self, state: dict):
        if "worker" in state and hasattr(self, "workers"):
            self.workers.local_worker().restore(state["worker"])
            remote_state = ray.put(state["worker"])
            for r in self.workers.remote_workers():
                r.restore.remote(remote_state)
        # Restore optimizer data, if necessary.
        if "optimizer" in state and hasattr(self, "optimizer"):
            self.optimizer.restore(state["optimizer"])
        # If necessary, restore replay data as well.
        if self.local_replay_buffer is not None:
            # TODO: Experimental functionality: Restore contents of replay
            #  buffer from checkpoint, only if user has configured this.
            if self.config.get("store_buffer_in_checkpoints"):
                if "local_replay_buffer" in state:
                    self.local_replay_buffer.set_state(
                        state["local_replay_buffer"])
                else:
                    logger.warning(
                        "`store_buffer_in_checkpoints` is True, but no replay "
                        "data found in state!")
            elif "local_replay_buffer" in state and \
                    log_once("no_store_buffer_in_checkpoints_but_data_found"):
                logger.warning(
                    "`store_buffer_in_checkpoints` is False, but some replay "
                    "data found in state!")

        if self.train_exec_impl is not None:
            self.train_exec_impl.shared_metrics.get().restore(
                state["train_exec_impl"])

    # TODO: Deprecate this method (`build_trainer` should no longer be used).
    @staticmethod
    def with_updates(**overrides) -> Type["Trainer"]:
        raise NotImplementedError(
            "`with_updates` may only be called on Trainer sub-classes "
            "that were generated via the `ray.rllib.agents.trainer_template."
            "build_trainer()` function (which has been deprecated)!")

    @DeveloperAPI
    def _create_local_replay_buffer_if_necessary(
            self, config: PartialTrainerConfigDict
    ) -> Optional[MultiAgentReplayBuffer]:
        """Create a MultiAgentReplayBuffer instance if necessary.

        Args:
            config: Algorithm-specific configuration data.

        Returns:
            MultiAgentReplayBuffer instance based on trainer config.
            None, if local replay buffer is not needed.
        """
        # These are the agents that utilizes a local replay buffer.
        if ("replay_buffer_config" not in config
                or not config["replay_buffer_config"]):
            # Does not need a replay buffer.
            return None

        replay_buffer_config = config["replay_buffer_config"]
        if ("type" not in replay_buffer_config
                or replay_buffer_config["type"] != "MultiAgentReplayBuffer"):
            # DistributedReplayBuffer coming soon.
            return None

        capacity = config.get("buffer_size", DEPRECATED_VALUE)
        if capacity != DEPRECATED_VALUE:
            # Print a deprecation warning.
            deprecation_warning(
                old="config['buffer_size']",
                new="config['replay_buffer_config']['capacity']",
                error=False)
        else:
            # Get capacity out of replay_buffer_config.
            capacity = replay_buffer_config["capacity"]

        # Configure prio. replay parameters.
        if config.get("prioritized_replay"):
            prio_args = {
                "prioritized_replay_alpha": config["prioritized_replay_alpha"],
                "prioritized_replay_beta": config["prioritized_replay_beta"],
                "prioritized_replay_eps": config["prioritized_replay_eps"],
            }
        # Switch off prioritization (alpha=0.0).
        else:
            prio_args = {"prioritized_replay_alpha": 0.0}

        return MultiAgentReplayBuffer(
            num_shards=1,
            learning_starts=config["learning_starts"],
            capacity=capacity,
            replay_batch_size=config["train_batch_size"],
            replay_mode=config["multiagent"]["replay_mode"],
            replay_sequence_length=config.get("replay_sequence_length", 1),
            replay_burn_in=config.get("burn_in", 0),
            replay_zero_init_states=config.get("zero_init_states", True),
            **prio_args)

    @DeveloperAPI
    def _kwargs_for_execution_plan(self):
        kwargs = {}
        if self.local_replay_buffer:
            kwargs["local_replay_buffer"] = self.local_replay_buffer
        return kwargs

    def _register_if_needed(self, env_object: Union[str, EnvType, None],
                            config) -> Optional[str]:
        if isinstance(env_object, str):
            return env_object
        elif isinstance(env_object, type):
            name = env_object.__name__

            if config.get("remote_worker_envs"):

                @ray.remote(num_cpus=0)
                class _wrapper(env_object):
                    # Add convenience `_get_spaces` and `_is_multi_agent`
                    # methods.
                    def _get_spaces(self):
                        return self.observation_space, self.action_space

                    def _is_multi_agent(self):
                        return isinstance(self, MultiAgentEnv)

                register_env(name, lambda cfg: _wrapper.remote(cfg))
            else:
                register_env(name, lambda cfg: env_object(cfg))
            return name
        elif env_object is None:
            return None
        raise ValueError(
            "{} is an invalid env specification. ".format(env_object) +
            "You can specify a custom env as either a class "
            "(e.g., YourEnvCls) or a registered env id (e.g., \"your_env\").")

    def _step_context(trainer):
        class StepCtx:
            def __enter__(self):
                # First call to stop, `result` is expected to be None ->
                # Start with self.failures=-1 -> set to 0 the very first call
                # to `self.stop()`.
                self.failures = -1

                self.time_start = time.time()
                self.sampled = 0
                self.trained = 0
                self.init_env_steps_sampled = trainer._counters[
                    NUM_ENV_STEPS_SAMPLED]
                self.init_env_steps_trained = trainer._counters[
                    NUM_ENV_STEPS_TRAINED]
                self.init_agent_steps_sampled = trainer._counters[
                    NUM_AGENT_STEPS_SAMPLED]
                self.init_agent_steps_trained = trainer._counters[
                    NUM_AGENT_STEPS_TRAINED]
                return self

            def __exit__(self, *args):
                pass

            def should_stop(self, result):

                # First call to stop, `result` is expected to be None ->
                # self.failures=0.
                if result is None:
                    # Fail after n retries.
                    self.failures += 1
                    if self.failures > MAX_WORKER_FAILURE_RETRIES:
                        raise RuntimeError(
                            "Failed to recover from worker crash.")

                # Stopping criteria: Only when using the `training_iteration`
                # API, b/c for the `exec_plan` API, the logic to stop is
                # already built into the execution plans via the
                # `StandardMetricsReporting` op.
                elif trainer.config["_disable_execution_plan_api"]:
                    if trainer._by_agent_steps:
                        self.sampled = \
                            trainer._counters[NUM_AGENT_STEPS_SAMPLED] - \
                            self.init_agent_steps_sampled
                        self.trained = \
                            trainer._counters[NUM_AGENT_STEPS_TRAINED] - \
                            self.init_agent_steps_trained
                    else:
                        self.sampled = \
                            trainer._counters[NUM_ENV_STEPS_SAMPLED] - \
                            self.init_env_steps_sampled
                        self.trained = \
                            trainer._counters[NUM_ENV_STEPS_TRAINED] - \
                            self.init_env_steps_trained

                    min_t = trainer.config["min_time_s_per_reporting"]
                    min_sample_ts = trainer.config[
                        "min_sample_timesteps_per_reporting"]
                    min_train_ts = trainer.config[
                        "min_train_timesteps_per_reporting"]
                    # Repeat if not enough time has passed or if not enough
                    # env|train timesteps have been processed (or these min
                    # values are not provided by the user).
                    if result is not None and \
                            (not min_t or
                             time.time() - self.time_start >= min_t) and \
                            (not min_sample_ts or
                             self.sampled >= min_sample_ts) and \
                            (not min_train_ts or
                             self.trained >= min_train_ts):
                        return True
                # No errors (we got results) -> Break.
                elif result is not None:
                    return True

                return False

        return StepCtx()

    def _compile_step_results(self, *, step_ctx, step_attempt_results=None):
        # Return dict.
        results: ResultDict = {}
        step_attempt_results = step_attempt_results or {}

        # Evaluation results.
        if "evaluation" in step_attempt_results:
            results["evaluation"] = step_attempt_results.pop("evaluation")

        # Custom metrics and episode media.
        results["custom_metrics"] = step_attempt_results.pop(
            "custom_metrics", {})
        results["episode_media"] = step_attempt_results.pop(
            "episode_media", {})

        # Learner info.
        results["info"] = {LEARNER_INFO: step_attempt_results}

        # Collect rollout worker metrics.
        episodes, self._episodes_to_be_collected = collect_episodes(
            self.workers.local_worker(),
            self.workers.remote_workers(),
            self._episodes_to_be_collected,
            timeout_seconds=self.config[
                "metrics_episode_collection_timeout_s"])
        orig_episodes = list(episodes)
        missing = self.config["metrics_num_episodes_for_smoothing"] - \
            len(episodes)
        if missing > 0:
            episodes = self._episode_history[-missing:] + episodes
            assert len(episodes) <= \
                   self.config["metrics_num_episodes_for_smoothing"]
        self._episode_history.extend(orig_episodes)
        self._episode_history = \
            self._episode_history[
                -self.config["metrics_num_episodes_for_smoothing"]:]
        results["sampler_results"] = summarize_episodes(
            episodes, orig_episodes)
        # TODO: Don't dump sampler results into top-level.
        results.update(results["sampler_results"])

        results["num_healthy_workers"] = len(self.workers.remote_workers())

        # Train-steps- and env/agent-steps this iteration.
        for c in [
                NUM_AGENT_STEPS_SAMPLED, NUM_AGENT_STEPS_TRAINED,
                NUM_ENV_STEPS_SAMPLED, NUM_ENV_STEPS_TRAINED
        ]:
            results[c] = self._counters[c]
        if self._by_agent_steps:
            results[NUM_AGENT_STEPS_SAMPLED + "_this_iter"] = step_ctx.sampled
            results[NUM_AGENT_STEPS_TRAINED + "_this_iter"] = step_ctx.trained
            # TODO: For CQL and other algos, count by trained steps.
            results["timesteps_total"] = self._counters[
                NUM_AGENT_STEPS_SAMPLED]
        else:
            results[NUM_ENV_STEPS_SAMPLED + "_this_iter"] = step_ctx.sampled
            results[NUM_ENV_STEPS_TRAINED + "_this_iter"] = step_ctx.trained
            # TODO: For CQL and other algos, count by trained steps.
            results["timesteps_total"] = self._counters[NUM_ENV_STEPS_SAMPLED]
        # TODO: Backward compatibility.
        results["agent_timesteps_total"] = self._counters[
            NUM_AGENT_STEPS_SAMPLED]

        # Process timer results.
        timers = {}
        for k, timer in self._timers.items():
            timers["{}_time_ms".format(k)] = round(timer.mean * 1000, 3)
            if timer.has_units_processed():
                timers["{}_throughput".format(k)] = round(
                    timer.mean_throughput, 3)
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

    @Deprecated(new="Trainer.evaluate()", error=True)
    def _evaluate(self) -> dict:
        return self.evaluate()

    @Deprecated(new="Trainer.compute_single_action()", error=False)
    def compute_action(self, *args, **kwargs):
        return self.compute_single_action(*args, **kwargs)

    @Deprecated(new="Trainer.try_recover_from_step_attempt()", error=False)
    def _try_recover(self):
        return self.try_recover_from_step_attempt()

    @staticmethod
    @Deprecated(new="Trainer.validate_config()", error=False)
    def _validate_config(config, trainer_or_none):
        assert trainer_or_none is not None
        return trainer_or_none.validate_config(config)

    # TODO: `self.optimizer` is no longer created in Trainer ->
    #  Deprecate this method.
    @Deprecated(error=False)
    def collect_metrics(self, selected_workers=None):
        return self.optimizer.collect_metrics(
            self.config["metrics_episode_collection_timeout_s"],
            min_history=self.config["metrics_num_episodes_for_smoothing"],
            selected_workers=selected_workers)

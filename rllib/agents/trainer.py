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
from packaging import version
import pickle
import pkg_resources
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
    Union,
)

import ray
from ray.actor import ActorHandle
from ray.exceptions import RayActorError, RayError
from ray.rllib.agents.callbacks import DefaultCallbacks
from ray.rllib.agents.trainer_config import TrainerConfig
from ray.rllib.env.env_context import EnvContext
from ray.rllib.env.utils import _gym_env_creator
from ray.rllib.evaluation.episode import Episode
from ray.rllib.utils import force_list
from ray.rllib.evaluation.metrics import (
    collect_episodes,
    collect_metrics,
    summarize_episodes,
)
from ray.rllib.evaluation.rollout_worker import RolloutWorker
from ray.rllib.evaluation.worker_set import WorkerSet
from ray.rllib.utils.replay_buffers import MultiAgentReplayBuffer
from ray.rllib.execution.common import WORKER_UPDATE_TIMER
from ray.rllib.execution.rollout_ops import (
    synchronous_parallel_sample,
)
from ray.rllib.execution.train_ops import (
    train_one_step,
    multi_gpu_train_one_step,
)
from ray.rllib.offline import get_offline_io_resource_bundles
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID, SampleBatch
from ray.rllib.utils import deep_update, FilterManager, merge_dicts
from ray.rllib.utils.annotations import (
    DeveloperAPI,
    ExperimentalAPI,
    override,
    OverrideToImplementCustomLogic,
    OverrideToImplementCustomLogic_CallToSuperRecommended,
    PublicAPI,
)
from ray.rllib.utils.debug import update_global_seed_if_necessary
from ray.rllib.utils.deprecation import (
    Deprecated,
    deprecation_warning,
    DEPRECATED_VALUE,
)
from ray.rllib.utils.error import EnvError, ERR_MSG_INVALID_ENV_DESCRIPTOR
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.from_config import from_config
from ray.rllib.utils.metrics import (
    TRAINING_ITERATION_TIMER,
    NUM_ENV_STEPS_SAMPLED,
    NUM_AGENT_STEPS_SAMPLED,
    NUM_ENV_STEPS_TRAINED,
    NUM_AGENT_STEPS_TRAINED,
)
from ray.rllib.utils.metrics.learner_info import LEARNER_INFO
from ray.rllib.utils.pre_checks.multi_agent import check_multi_agent
from ray.rllib.utils.spaces import space_utils
from ray.rllib.utils.typing import (
    AgentID,
    EnvCreator,
    EnvInfoDict,
    EnvType,
    EpisodeID,
    PartialTrainerConfigDict,
    PolicyID,
    PolicyState,
    ResultDict,
    SampleBatchType,
    TensorStructType,
    TensorType,
    TrainerConfigDict,
)
from ray.tune.logger import Logger, UnifiedLogger
from ray.tune.registry import ENV_CREATOR, _global_registry
from ray.tune.resources import Resources
from ray.tune.result import DEFAULT_RESULTS_DIR
from ray.tune.trainable import Trainable
from ray.tune.trial import ExportFormat
from ray.tune.utils.placement_groups import PlacementGroupFactory
from ray.util import log_once
from ray.util.timer import _Timer

tf1, tf, tfv = try_import_tf()

logger = logging.getLogger(__name__)


@DeveloperAPI
def with_common_config(extra_config: PartialTrainerConfigDict) -> TrainerConfigDict:
    """Returns the given config dict merged with common agent confs.

    Args:
        extra_config: A user defined partial config
            which will get merged with a default TrainerConfig() object and returned
            as plain python dict.

    Returns:
        TrainerConfigDict: The merged config dict resulting from TrainerConfig()
            plus `extra_config`.
    """
    return Trainer.merge_trainer_configs(
        TrainerConfig().to_dict(), extra_config, _allow_unknown_configs=True
    )


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
    e.g. rllib.algorithms.dqn.dqn.py or rllib.agents.impala.impala.py.

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
        "tf_session_args",
        "local_tf_session_args",
        "env_config",
        "model",
        "optimizer",
        "multiagent",
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

    @PublicAPI
    def __init__(
        self,
        config: Optional[Union[PartialTrainerConfigDict, TrainerConfig]] = None,
        env: Optional[Union[str, EnvType]] = None,
        logger_creator: Optional[Callable[[], Logger]] = None,
        remote_checkpoint_dir: Optional[str] = None,
        sync_function_tpl: Optional[str] = None,
    ):
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
        # Trainer's Config object). Will get merged with TrainerConfig()
        # in self.setup().
        config = config or {}
        # Resolve TrainerConfig into a plain dict.
        # TODO: In the future, only support TrainerConfig objects here.
        if isinstance(config, TrainerConfig):
            config = config.to_dict()

        # Convert `env` provided in config into a concrete env creator callable, which
        # takes an EnvContext (config dict) as arg and returning an RLlib supported Env
        # type (e.g. a gym.Env).
        self._env_id, self.env_creator = self._get_env_id_and_creator(
            env or config.get("env"), config
        )
        env_descr = (
            self._env_id.__name__ if isinstance(self._env_id, type) else self._env_id
        )

        # Placeholder for a local replay buffer instance.
        self.local_replay_buffer = None

        # Create a default logger creator if no logger_creator is specified
        if logger_creator is None:
            # Default logdir prefix containing the agent's name and the
            # env id.
            timestr = datetime.today().strftime("%Y-%m-%d_%H-%M-%S")
            logdir_prefix = "{}_{}_{}".format(str(self), env_descr, timestr)
            if not os.path.exists(DEFAULT_RESULTS_DIR):
                os.makedirs(DEFAULT_RESULTS_DIR)
            logdir = tempfile.mkdtemp(prefix=logdir_prefix, dir=DEFAULT_RESULTS_DIR)

            # Allow users to more precisely configure the created logger
            # via "logger_config.type".
            if config.get("logger_config") and "type" in config["logger_config"]:

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
        self._remote_workers_for_metrics = []

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

        super().__init__(
            config, logger_creator, remote_checkpoint_dir, sync_function_tpl
        )

    @OverrideToImplementCustomLogic
    @classmethod
    def get_default_config(cls) -> TrainerConfigDict:
        return TrainerConfig().to_dict()

    @OverrideToImplementCustomLogic_CallToSuperRecommended
    @override(Trainable)
    def setup(self, config: PartialTrainerConfigDict):

        # Setup our config: Merge the user-supplied config (which could
        # be a partial config dict with the class' default).
        self.config = self.merge_trainer_configs(
            self.get_default_config(), config, self._allow_unknown_configs
        )
        self.config["env"] = self._env_id

        # Validate the framework settings in config.
        self.validate_framework(self.config)

        # Set Trainer's seed after we have - if necessary - enabled
        # tf eager-execution.
        update_global_seed_if_necessary(self.config["framework"], self.config["seed"])

        self.validate_config(self.config)
        self.callbacks = self.config["callbacks"]()
        log_level = self.config.get("log_level")
        if log_level in ["WARN", "ERROR"]:
            logger.info(
                "Current log_level is {}. For more information, "
                "set 'log_level': 'INFO' / 'DEBUG' or use the -v and "
                "-vv flags.".format(log_level)
            )
        if self.config.get("log_level"):
            logging.getLogger("ray.rllib").setLevel(self.config["log_level"])

        # Create local replay buffer if necessary.
        self.local_replay_buffer = self._create_local_replay_buffer_if_necessary(
            self.config
        )

        # Create a dict, mapping ActorHandles to sets of open remote
        # requests (object refs). This way, we keep track, of which actors
        # inside this Trainer (e.g. a remote RolloutWorker) have
        # already been sent how many (e.g. `sample()`) requests.
        self.remote_requests_in_flight: DefaultDict[
            ActorHandle, Set[ray.ObjectRef]
        ] = defaultdict(set)

        self.workers: Optional[WorkerSet] = None
        self.train_exec_impl = None

        # Deprecated way of implementing Trainer sub-classes (or "templates"
        # via the `build_trainer` utility function).
        # Instead, sub-classes should override the Trainable's `setup()`
        # method and call super().setup() from within that override at some
        # point.
        # Old design: Override `Trainer._init`.
        _init = False
        try:
            self._init(self.config, self.env_creator)
            _init = True
        # New design: Override `Trainable.setup()` (as indented by tune.Trainable)
        # and do or don't call `super().setup()` from within your override.
        # By default, `super().setup()` will create both worker sets:
        # "rollout workers" for collecting samples for training and - if
        # applicable - "evaluation workers" for evaluation runs in between or
        # parallel to training.
        # TODO: Deprecate `_init()` and remove this try/except block.
        except NotImplementedError:
            pass

        # Only if user did not override `_init()`:
        if _init is False:
            # - Create rollout workers here automatically.
            # - Run the execution plan to create the local iterator to `next()`
            #   in each training iteration.
            # This matches the behavior of using `build_trainer()`, which
            # has been deprecated.
            try:
                self.workers = WorkerSet(
                    env_creator=self.env_creator,
                    validate_env=self.validate_env,
                    policy_class=self.get_default_policy_class(self.config),
                    trainer_config=self.config,
                    num_workers=self.config["num_workers"],
                    local_worker=True,
                    logdir=self.logdir,
                )
            # WorkerSet creation possibly fails, if some (remote) workers cannot
            # be initialized properly (due to some errors in the RolloutWorker's
            # constructor).
            except RayActorError as e:
                # In case of an actor (remote worker) init failure, the remote worker
                # may still exist and will be accessible, however, e.g. calling
                # its `sample.remote()` would result in strange "property not found"
                # errors.
                if e.actor_init_failed:
                    # Raise the original error here that the RolloutWorker raised
                    # during its construction process. This is to enforce transparency
                    # for the user (better to understand the real reason behind the
                    # failure).
                    # - e.args[0]: The RayTaskError (inside the caught RayActorError).
                    # - e.args[0].args[2]: The original Exception (e.g. a ValueError due
                    # to a config mismatch) thrown inside the actor.
                    raise e.args[0].args[2]
                # In any other case, raise the RayActorError as-is.
                else:
                    raise e
            # By default, collect metrics for all remote workers.
            self._remote_workers_for_metrics = self.workers.remote_workers()

            # Function defining one single training iteration's behavior.
            if self.config["_disable_execution_plan_api"]:
                # Ensure remote workers are initially in sync with the local worker.
                self.workers.sync_weights()
            # LocalIterator-creating "execution plan".
            # Only call this once here to create `self.train_exec_impl`,
            # which is a ray.util.iter.LocalIterator that will be `next`'d
            # on each training iteration.
            else:
                self.train_exec_impl = self.execution_plan(
                    self.workers, self.config, **self._kwargs_for_execution_plan()
                )

            # Now that workers have been created, update our policies
            # dict in config[multiagent] (with the correct original/
            # unpreprocessed spaces).
            self.config["multiagent"][
                "policies"
            ] = self.workers.local_worker().policy_dict

        # Evaluation WorkerSet setup.
        # User would like to setup a separate evaluation worker set.

        # Update with evaluation settings:
        user_eval_config = copy.deepcopy(self.config["evaluation_config"])

        # Assert that user has not unset "in_evaluation".
        assert (
            "in_evaluation" not in user_eval_config
            or user_eval_config["in_evaluation"] is True
        )

        # Merge user-provided eval config with the base config. This makes sure
        # the eval config is always complete, no matter whether we have eval
        # workers or perform evaluation on the (non-eval) local worker.
        eval_config = merge_dicts(self.config, user_eval_config)
        self.config["evaluation_config"] = eval_config

        if self.config.get("evaluation_num_workers", 0) > 0 or self.config.get(
            "evaluation_interval"
        ):
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
                eval_config.update(
                    {
                        "batch_mode": "complete_episodes",
                        "rollout_fragment_length": 1,
                    }
                )
            # Evaluation duration unit: timesteps.
            # - Set `batch_mode=truncate_episodes` so we don't perform rollouts
            #   strictly along episode borders.
            # Set `rollout_fragment_length` such that desired steps are divided
            # equally amongst workers or - in "auto" duration mode - set it
            # to a reasonably small number (10), such that a single `sample()`
            # call doesn't take too much time so we can stop evaluation as soon
            # as possible after the train step is completed.
            else:
                eval_config.update(
                    {
                        "batch_mode": "truncate_episodes",
                        "rollout_fragment_length": 10
                        if self.config["evaluation_duration"] == "auto"
                        else int(
                            math.ceil(
                                self.config["evaluation_duration"]
                                / (self.config["evaluation_num_workers"] or 1)
                            )
                        ),
                    }
                )

            self.config["evaluation_config"] = eval_config

            env_id, env_creator = self._get_env_id_and_creator(
                eval_config.get("env"), eval_config
            )

            # Create a separate evaluation worker set for evaluation.
            # If evaluation_num_workers=0, use the evaluation set's local
            # worker for evaluation, otherwise, use its remote workers
            # (parallelized evaluation).
            self.evaluation_workers: WorkerSet = WorkerSet(
                env_creator=env_creator,
                validate_env=None,
                policy_class=self.get_default_policy_class(self.config),
                trainer_config=eval_config,
                num_workers=self.config["evaluation_num_workers"],
                # Don't even create a local worker if num_workers > 0.
                local_worker=False,
                logdir=self.logdir,
            )

        # Run any callbacks after trainer initialization is done.
        self.callbacks.on_trainer_init(trainer=self)

    # TODO: Deprecated: In your sub-classes of Trainer, override `setup()`
    #  directly and call super().setup() from within it if you would like the
    #  default setup behavior plus some own setup logic.
    #  If you don't need the env/workers/config/etc.. setup for you by super,
    #  simply do not call super().setup() from your overridden method.
    def _init(self, config: TrainerConfigDict, env_creator: EnvCreator) -> None:
        raise NotImplementedError

    @OverrideToImplementCustomLogic
    def get_default_policy_class(self, config: TrainerConfigDict) -> Type[Policy]:
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
        self._rollout_worker_metrics = []
        local_worker = (
            self.workers.local_worker()
            if hasattr(self.workers, "local_worker")
            else None
        )
        with self._step_context() as step_ctx:
            while not step_ctx.should_stop(step_attempt_results):
                # Try to train one step.
                try:
                    step_attempt_results = self.step_attempt()
                    # Collect rollout worker metrics.
                    episodes, self._episodes_to_be_collected = collect_episodes(
                        local_worker,
                        self._remote_workers_for_metrics,
                        self._episodes_to_be_collected,
                        timeout_seconds=self.config[
                            "metrics_episode_collection_timeout_s"
                        ],
                    )
                    self._rollout_worker_metrics.extend(episodes)
                # @ray.remote RolloutWorker failure.
                except RayError as e:
                    # Try to recover w/o the failed worker.
                    if (
                        self.config["ignore_worker_failures"]
                        or self.config["recreate_failed_workers"]
                    ):
                        logger.exception("Error in train call, attempting to recover")
                        self.try_recover_from_step_attempt()
                    # Error out.
                    else:
                        logger.warning(
                            "Worker crashed during call to `step_attempt()`. "
                            "To try to continue training without failed "
                            "worker(s), set `ignore_worker_failures=True`. "
                            "To try to recover the failed worker(s), set "
                            "`recreate_failed_workers=True`."
                        )
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

        # Check `env_task_fn` for possible update of the env's task.
        if self.config["env_task_fn"] is not None:
            if not callable(self.config["env_task_fn"]):
                raise ValueError(
                    "`env_task_fn` must be None or a callable taking "
                    "[train_results, env, env_ctx] as args!"
                )

            def fn(env, env_context, task_fn):
                new_task = task_fn(result, env, env_context)
                cur_task = env.get_task()
                if cur_task != new_task:
                    env.set_task(new_task)

            fn = functools.partial(fn, task_fn=self.config["env_task_fn"])
            self.workers.foreach_env_with_context(fn)

        return result

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
            if num_units_done > 0 and train_future.done():
                return 0
            # Count by episodes. -> Run n more
            # (n=num eval workers).
            elif unit == "episodes":
                return num_eval_workers
            # Count by timesteps. -> Run n*m*p more
            # (n=num eval workers; m=rollout fragment length;
            # p=num-envs-per-worker).
            else:
                return (
                    num_eval_workers
                    * eval_cfg["rollout_fragment_length"]
                    * eval_cfg["num_envs_per_worker"]
                )

        # self._iteration gets incremented after this function returns,
        # meaning that e. g. the first time this function is called,
        # self._iteration will be 0.
        evaluate_this_iter = (
            self.config["evaluation_interval"]
            and (self._iteration + 1) % self.config["evaluation_interval"] == 0
        )

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
                        lambda: self._exec_plan_or_training_iteration_fn()
                    )
                    # Automatically determine duration of the evaluation.
                    if self.config["evaluation_duration"] == "auto":
                        unit = self.config["evaluation_duration_unit"]
                        step_results.update(
                            self.evaluate(
                                duration_fn=functools.partial(
                                    auto_duration_fn,
                                    unit,
                                    self.config["evaluation_num_workers"],
                                    self.config["evaluation_config"],
                                )
                            )
                        )
                    else:
                        step_results.update(self.evaluate())
                    # Collect the training results from the future.
                    step_results.update(train_future.result())
            # Sequential: train (already done above), then eval.
            else:
                step_results.update(self.evaluate())

        # Attach latest available evaluation results to train results,
        # if necessary.
        if not evaluate_this_iter and self.config["always_attach_evaluation_results"]:
            assert isinstance(
                self.evaluation_metrics, dict
            ), "Trainer.evaluate() needs to return a dict."
            step_results.update(self.evaluation_metrics)

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
                error=False,
            )
            duration_fn = episodes_left_fn

        # In case we are evaluating (in a thread) parallel to training,
        # we may have to re-enable eager mode here (gets disabled in the
        # thread).
        if (
            self.config.get("framework") in ["tf2", "tfe"]
            and not tf.executing_eagerly()
        ):
            tf1.enable_eager_execution()

        # Call the `_before_evaluate` hook.
        self._before_evaluate()

        # Sync weights to the evaluation WorkerSet.
        if self.evaluation_workers is not None:
            self.evaluation_workers.sync_weights(
                from_worker=self.workers.local_worker()
            )
            self._sync_filters_if_needed(self.evaluation_workers)

        if self.config["custom_eval_function"]:
            logger.info(
                "Running custom eval function {}".format(
                    self.config["custom_eval_function"]
                )
            )
            metrics = self.config["custom_eval_function"](self, self.evaluation_workers)
            if not metrics or not isinstance(metrics, dict):
                raise ValueError(
                    "Custom eval function must return "
                    "dict of metrics, got {}.".format(metrics)
                )
        else:
            if (
                self.evaluation_workers is None
                and self.workers.local_worker().input_reader is None
            ):
                raise ValueError(
                    "Cannot evaluate w/o an evaluation worker set in "
                    "the Trainer or w/o an env on the local worker!\n"
                    "Try one of the following:\n1) Set "
                    "`evaluation_interval` >= 0 to force creating a "
                    "separate evaluation worker set.\n2) Set "
                    "`create_env_on_driver=True` to force the local "
                    "(non-eval) worker to have an environment to "
                    "evaluate on."
                )

            # How many episodes/timesteps do we need to run?
            # In "auto" mode (only for parallel eval + training): Run as long
            # as training lasts.
            unit = self.config["evaluation_duration_unit"]
            eval_cfg = self.config["evaluation_config"]
            rollout = eval_cfg["rollout_fragment_length"]
            num_envs = eval_cfg["num_envs_per_worker"]
            duration = (
                self.config["evaluation_duration"]
                if self.config["evaluation_duration"] != "auto"
                else (self.config["evaluation_num_workers"] or 1)
                * (1 if unit == "episodes" else rollout)
            )
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
                metrics = collect_metrics(
                    self.workers.local_worker(),
                    keep_custom_metrics=self.config["keep_per_episode_custom_metrics"],
                )

            # Evaluation worker set only has local worker.
            elif self.config["evaluation_num_workers"] == 0:
                # If unit=episodes -> Run n times `sample()` (each sample
                # produces exactly 1 episode).
                # If unit=ts -> Run 1 `sample()` b/c the
                # `rollout_fragment_length` is exactly the desired ts.
                iters = duration if unit == "episodes" else 1
                for _ in range(iters):
                    num_ts_run += len(self.evaluation_workers.local_worker().sample())

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
                    batches = ray.get(
                        [
                            w.sample.remote()
                            for i, w in enumerate(
                                self.evaluation_workers.remote_workers()
                            )
                            if i * (1 if unit == "episodes" else rollout * num_envs)
                            < units_left_to_do
                        ]
                    )
                    # 1 episode per returned batch.
                    if unit == "episodes":
                        num_units_done += len(batches)
                    # n timesteps per returned batch.
                    else:
                        ts = sum(len(b) for b in batches)
                        num_ts_run += ts
                        num_units_done += ts

                    logger.info(
                        f"Ran round {round_} of parallel evaluation "
                        f"({num_units_done}/{duration} {unit} done)"
                    )

            if metrics is None:
                metrics = collect_metrics(
                    self.evaluation_workers.local_worker(),
                    self.evaluation_workers.remote_workers(),
                    keep_custom_metrics=self.config["keep_per_episode_custom_metrics"],
                )
            metrics["timesteps_this_iter"] = num_ts_run

        # Evaluation does not run for every step.
        # Save evaluation metrics on trainer, so it can be attached to
        # subsequent step results as latest evaluation result.
        self.evaluation_metrics = {"evaluation": metrics}

        # Also return the results here for convenience.
        return self.evaluation_metrics

    @OverrideToImplementCustomLogic
    @DeveloperAPI
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
        # Collect SampleBatches from sample workers until we have a full batch.
        if self._by_agent_steps:
            train_batch = synchronous_parallel_sample(
                worker_set=self.workers, max_agent_steps=self.config["train_batch_size"]
            )
        else:
            train_batch = synchronous_parallel_sample(
                worker_set=self.workers, max_env_steps=self.config["train_batch_size"]
            )
        train_batch = train_batch.as_multi_agent()
        self._counters[NUM_AGENT_STEPS_SAMPLED] += train_batch.agent_steps()
        self._counters[NUM_ENV_STEPS_SAMPLED] += train_batch.env_steps()

        # Use simple optimizer (only for multi-agent or tf-eager; all other
        # cases should use the multi-GPU optimizer, even if only using 1 GPU).
        # TODO: (sven) rename MultiGPUOptimizer into something more
        #  meaningful.
        if self.config.get("simple_optimizer") is True:
            train_results = train_one_step(self, train_batch)
        else:
            train_results = multi_gpu_train_one_step(self, train_batch)

        # Update weights and global_vars - after learning on the local worker - on all
        # remote workers.
        global_vars = {
            "timestep": self._counters[NUM_ENV_STEPS_SAMPLED],
        }
        with self._timers[WORKER_UPDATE_TIMER]:
            self.workers.sync_weights(global_vars=global_vars)

        return train_results

    @staticmethod
    def execution_plan(workers, config, **kwargs):
        raise NotImplementedError(
            "It is not longer recommended to use Trainer's `execution_plan` method/API."
            " Set `_disable_execution_plan_api=True` in your config and override the "
            "`Trainer.training_iteration()` method with your algo's custom "
            "execution logic."
        )

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
                error=False,
            )
            clip_action = clip_actions
        if unsquash_actions != DEPRECATED_VALUE:
            deprecation_warning(
                old="Trainer.compute_single_action(`unsquash_actions`=...)",
                new="Trainer.compute_single_action(`unsquash_action`=...)",
                error=False,
            )
            unsquash_action = unsquash_actions

        # `unsquash_action` is None: Use value of config['normalize_actions'].
        if unsquash_action is None:
            unsquash_action = self.config["normalize_actions"]
        # `clip_action` is None: Use value of config['clip_actions'].
        elif clip_action is None:
            clip_action = self.config["clip_actions"]

        # User provided an input-dict: Assert that `obs`, `prev_a|r`, `state`
        # are all None.
        err_msg = (
            "Provide either `input_dict` OR [`observation`, ...] as "
            "args to Trainer.compute_single_action!"
        )
        if input_dict is not None:
            assert (
                observation is None
                and prev_action is None
                and prev_reward is None
                and state is None
            ), err_msg
            observation = input_dict[SampleBatch.OBS]
        else:
            assert observation is not None, err_msg

        # Get the policy to compute the action for (in the multi-agent case,
        # Trainer may hold >1 policies).
        policy = self.get_policy(policy_id)
        if policy is None:
            raise KeyError(
                f"PolicyID '{policy_id}' not found in PolicyMap of the "
                f"Trainer's local worker!"
            )
        local_worker = self.workers.local_worker()

        # Check the preprocessor and preprocess, if necessary.
        pp = local_worker.preprocessors[policy_id]
        if pp and type(pp).__name__ != "NoPreprocessor":
            observation = pp.transform(observation)
        observation = local_worker.filters[policy_id](observation, update=False)

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
                error=False,
            )
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

        # prev_action and prev_reward can be None, np.ndarray, or tensor-like structure.
        # Explicitly check for None here to avoid the error message "The truth value of
        # an array with more than one element is ambiguous.", when np arrays are passed
        # as arguments.
        if prev_action is not None:
            input_dict[SampleBatch.PREV_ACTIONS] = prev_action
        if prev_reward is not None:
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
        policy_state: Optional[PolicyState] = None,
        policy_mapping_fn: Optional[Callable[[AgentID, EpisodeID], PolicyID]] = None,
        policies_to_train: Optional[
            Union[
                Container[PolicyID],
                Callable[[PolicyID, Optional[SampleBatchType]], bool],
            ]
        ] = None,
        evaluation_workers: bool = True,
        workers: Optional[List[Union[RolloutWorker, ActorHandle]]] = None,
    ) -> Policy:
        """Adds a new policy to this Trainer.

        Args:
            policy_id: ID of the policy to add.
            policy_cls: The Policy class to use for
                constructing the new Policy.
            observation_space: The observation space of the policy to add.
                If None, try to infer this space from the environment.
            action_space: The action space of the policy to add.
                If None, try to infer this space from the environment.
            config: The config overrides for the policy to add.
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
                to the evaluation WorkerSet.
            workers: A list of RolloutWorker/ActorHandles (remote
                RolloutWorkers) to add this policy to. If defined, will only
                add the given policy to these workers.

        Returns:
            The newly added policy (the copy that got added to the local
            worker).
        """

        kwargs = dict(
            policy_id=policy_id,
            policy_cls=policy_cls,
            observation_space=observation_space,
            action_space=action_space,
            config=config,
            policy_state=policy_state,
            policy_mapping_fn=policy_mapping_fn,
            policies_to_train=list(policies_to_train) if policies_to_train else None,
        )

        def fn(worker: RolloutWorker):
            # `foreach_worker` function: Adds the policy the the worker (and
            # maybe changes its policy_mapping_fn - if provided here).
            worker.add_policy(**kwargs)

        if workers is not None:
            ray_gets = []
            for worker in workers:
                if isinstance(worker, ActorHandle):
                    ray_gets.append(worker.add_policy.remote(**kwargs))
                else:
                    fn(worker)
            ray.get(ray_gets)
        else:
            # Run foreach_worker fn on all workers.
            self.workers.foreach_worker(fn)

        # Update evaluation workers, if necessary.
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
        policies_to_train: Optional[
            Union[Set[PolicyID], Callable[[PolicyID, Optional[SampleBatchType]], bool]]
        ] = None,
        evaluation_workers: bool = True,
    ) -> None:
        """Removes a new policy from this Trainer.

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
                evaluation WorkerSet.
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

        Example:
            >>> from ray.rllib.algorithms.ppo import PPO
            >>> # Use a Trainer from RLlib or define your own.
            >>> trainer = PPO(...) # doctest: +SKIP
            >>> for _ in range(10): # doctest: +SKIP
            >>>     trainer.train() # doctest: +SKIP
            >>> trainer.export_policy_model("/tmp/dir") # doctest: +SKIP
            >>> trainer.export_policy_model("/tmp/dir/onnx", onnx=1) # doctest: +SKIP
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
            >>> from ray.rllib.algorithms.ppo import PPO
            >>> # Use a Trainer from RLlib or define your own.
            >>> trainer = PPO(...) # doctest: +SKIP
            >>> for _ in range(10): # doctest: +SKIP
            >>>     trainer.train() # doctest: +SKIP
            >>> trainer.export_policy_checkpoint("/tmp/export_dir") # doctest: +SKIP
        """
        self.get_policy(policy_id).export_checkpoint(export_dir, filename_prefix)

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
            >>> from ray.rllib.algorithms.ppo import PPO
            >>> trainer = PPO(...) # doctest: +SKIP
            >>> trainer.import_policy_model_from_h5("/tmp/weights.h5") # doctest: +SKIP
            >>> for _ in range(10): # doctest: +SKIP
            >>>     trainer.train() # doctest: +SKIP
        """
        self.get_policy(policy_id).import_model_from_h5(import_file)
        # Sync new weights to remote workers.
        self._sync_weights_to_workers(worker_set=self.workers)

    @override(Trainable)
    def save_checkpoint(self, checkpoint_dir: str) -> str:
        checkpoint_path = os.path.join(
            checkpoint_dir, "checkpoint-{}".format(self.iteration)
        )
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
        if hasattr(self, "workers") and self.workers is not None:
            self.workers.stop()
        if hasattr(self, "evaluation_workers") and self.evaluation_workers is not None:
            self.evaluation_workers.stop()

    @OverrideToImplementCustomLogic
    @classmethod
    @override(Trainable)
    def default_resource_request(
        cls, config: PartialTrainerConfigDict
    ) -> Union[Resources, PlacementGroupFactory]:

        # Default logic for RLlib algorithms (Trainers):
        # Create one bundle per individual worker (local or remote).
        # Use `num_cpus_for_driver` and `num_gpus` for the local worker and
        # `num_cpus_per_worker` and `num_gpus_per_worker` for the remote
        # workers to determine their CPU/GPU resource needs.

        # Convenience config handles.
        cf = dict(cls.get_default_config(), **config)
        eval_cf = cf["evaluation_config"]

        local_worker = {
            "CPU": cf["num_cpus_for_driver"],
            "GPU": 0 if cf["_fake_gpus"] else cf["num_gpus"],
        }
        rollout_workers = [
            {
                "CPU": cf["num_cpus_per_worker"],
                "GPU": cf["num_gpus_per_worker"],
                **cf["custom_resources_per_worker"],
            }
            for _ in range(cf["num_workers"])
        ]

        bundles = [local_worker] + rollout_workers

        if cf["evaluation_interval"]:
            # Evaluation workers.
            # Note: The local eval worker is located on the driver CPU.
            bundles += [
                {
                    "CPU": eval_cf.get(
                        "num_cpus_per_worker", cf["num_cpus_per_worker"]
                    ),
                    "GPU": eval_cf.get(
                        "num_gpus_per_worker", cf["num_gpus_per_worker"]
                    ),
                    **eval_cf.get(
                        "custom_resources_per_worker", cf["custom_resources_per_worker"]
                    ),
                }
                for _ in range(cf["evaluation_num_workers"])
            ]

        # In case our I/O reader/writer requires conmpute resources.
        bundles += get_offline_io_resource_bundles(cf)

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
        env_specifier: Union[str, EnvType, None], config: PartialTrainerConfigDict
    ) -> Tuple[Optional[str], EnvCreator]:
        """Returns env_id and creator callable given original env id from config.

        Args:
            env_specifier: An env class, an already tune registered env ID, a known
                gym env name, or None (if no env is used).
            config: The Trainer's (maybe partial) config dict.

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
            # Try gym/PyBullet/Vizdoom.
            else:
                return env_specifier, functools.partial(
                    _gym_env_creator, env_descriptor=env_specifier
                )

        elif isinstance(env_specifier, type):
            env_id = env_specifier  # .__name__

            if config.get("remote_worker_envs"):
                # Check gym version (0.22 or higher?).
                # If > 0.21, can't perform auto-wrapping of the given class as this
                # would lead to a pickle error.
                gym_version = pkg_resources.get_distribution("gym").version
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

    def _sync_filters_if_needed(self, workers: WorkerSet):
        if self.config.get("observation_filter", "NoFilter") != "NoFilter":
            FilterManager.synchronize(
                workers.local_worker().filters,
                workers.remote_workers(),
                update_remote=self.config["synchronize_filters"],
            )
            logger.debug(
                "synchronized filters: {}".format(workers.local_worker().filters)
            )

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
        with self._timers[TRAINING_ITERATION_TIMER]:
            if self.config["_disable_execution_plan_api"]:
                results = self.training_iteration()
            else:
                results = next(self.train_exec_impl)
        return results

    @classmethod
    @override(Trainable)
    def resource_help(cls, config: TrainerConfigDict) -> str:
        return (
            "\n\nYou can adjust the resource requests of RLlib agents by "
            "setting `num_workers`, `num_gpus`, and other configs. See "
            "the DEFAULT_CONFIG defined by each agent for more info.\n\n"
            "The config of this agent is: {}".format(config)
        )

    @classmethod
    def merge_trainer_configs(
        cls,
        config1: TrainerConfigDict,
        config2: PartialTrainerConfigDict,
        _allow_unknown_configs: Optional[bool] = None,
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
                return DefaultCallbacks(legacy_callbacks_dict=legacy_callbacks_dict)

            config2["callbacks"] = make_callbacks
        if _allow_unknown_configs is None:
            _allow_unknown_configs = cls._allow_unknown_configs
        return deep_update(
            config1,
            config2,
            _allow_unknown_configs,
            cls._allow_unknown_subkeys,
            cls._override_all_subkeys_if_type_changes,
        )

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
            """Check if tf/torch experiment is running and tf/torch installed."""
            if framework in tf_valid_frameworks:
                if not (_tf1 or _tf):
                    raise ImportError(
                        (
                            "TensorFlow was specified as the 'framework' "
                            "inside of your config dictionary. However, there was "
                            "no installation found. You can install TensorFlow "
                            "via `pip install tensorflow`"
                        )
                    )
            elif framework == "torch":
                if not _torch:
                    raise ImportError(
                        (
                            "PyTorch was specified as the 'framework' inside "
                            "of your config dictionary. However, there was no "
                            "installation found. You can install PyTorch via "
                            "`pip install torch`"
                        )
                    )

        def resolve_tf_settings():
            """Check and resolve tf settings."""

            if _tf1 and config["framework"] in ["tf2", "tfe"]:
                if config["framework"] == "tf2" and _tfv < 2:
                    raise ValueError(
                        "You configured `framework`=tf2, but your installed "
                        "pip tf-version is < 2.0! Make sure your TensorFlow "
                        "version is >= 2.x."
                    )
                if not _tf1.executing_eagerly():
                    _tf1.enable_eager_execution()
                # Recommend setting tracing to True for speedups.
                logger.info(
                    f"Executing eagerly (framework='{config['framework']}'),"
                    f" with eager_tracing={config['eager_tracing']}. For "
                    "production workloads, make sure to set eager_tracing=True"
                    "  in order to match the speed of tf-static-graph "
                    "(framework='tf'). For debugging purposes, "
                    "`eager_tracing=False` is the best choice."
                )
            # Tf-static-graph (framework=tf): Recommend upgrading to tf2 and
            # enabling eager tracing for similar speed.
            elif _tf1 and config["framework"] == "tf":
                logger.info(
                    "Your framework setting is 'tf', meaning you are using "
                    "static-graph mode. Set framework='tf2' to enable eager "
                    "execution with tf2.x. You may also then want to set "
                    "eager_tracing=True in order to reach similar execution "
                    "speed as with static-graph mode."
                )

        check_if_correct_nn_framework_installed()
        resolve_tf_settings()

    @OverrideToImplementCustomLogic_CallToSuperRecommended
    @DeveloperAPI
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

        # Use DefaultCallbacks class, if callbacks is None.
        if config["callbacks"] is None:
            config["callbacks"] = DefaultCallbacks
        # Check, whether given `callbacks` is a callable.
        if not callable(config["callbacks"]):
            raise ValueError(
                "`callbacks` must be a callable method that "
                "returns a subclass of DefaultCallbacks, got "
                f"{config['callbacks']}!"
            )

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
                raise ValueError(
                    "`num_gpus` > 1 not supported yet for "
                    "framework={}!".format(framework)
                )
            elif simple_optim_setting is True:
                raise ValueError(
                    "Cannot use `simple_optimizer` if `num_gpus` > 1! "
                    "Consider not setting `simple_optimizer` in your config."
                )
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
                if any(
                    (p[0] or default_policy_cls) is None
                    or not issubclass(
                        p[0] or default_policy_cls, (DynamicTFPolicy, TorchPolicy)
                    )
                    for p in config["multiagent"]["policies"].values()
                ):
                    config["simple_optimizer"] = True
                else:
                    config["simple_optimizer"] = False
            else:
                config["simple_optimizer"] = False

        # User manually set simple-optimizer to False -> Error if tf-eager.
        elif simple_optim_setting is False:
            if framework in ["tfe", "tf2"]:
                raise ValueError(
                    "`simple_optimizer=False` not supported for "
                    "framework={}!".format(framework)
                )

        # Offline RL settings.
        input_evaluation = config.get("input_evaluation")
        if input_evaluation is not None and input_evaluation is not DEPRECATED_VALUE:
            deprecation_warning(
                old="config.input_evaluation: {}".format(input_evaluation),
                new="config.off_policy_estimation_methods={}".format(input_evaluation),
                error=False,
            )
            config["off_policy_estimation_methods"] = input_evaluation
        config["off_policy_estimation_methods"] = force_list(
            config["off_policy_estimation_methods"]
        )

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
        prev_a_r = model_config.get("lstm_use_prev_action_reward", DEPRECATED_VALUE)
        if prev_a_r != DEPRECATED_VALUE:
            deprecation_warning(
                "model.lstm_use_prev_action_reward",
                "model.lstm_use_prev_action and model.lstm_use_prev_reward",
                error=False,
            )
            model_config["lstm_use_prev_action"] = prev_a_r
            model_config["lstm_use_prev_reward"] = prev_a_r

        # Check batching/sample collection settings.
        if config["batch_mode"] not in ["truncate_episodes", "complete_episodes"]:
            raise ValueError(
                "`batch_mode` must be one of [truncate_episodes|"
                "complete_episodes]! Got {}".format(config["batch_mode"])
            )

        # Store multi-agent batch count mode.
        self._by_agent_steps = (
            self.config["multiagent"].get("count_steps_by") == "agent_steps"
        )

        # Metrics settings.
        if (
            config.get("metrics_smoothing_episodes", DEPRECATED_VALUE)
            != DEPRECATED_VALUE
        ):
            deprecation_warning(
                old="metrics_smoothing_episodes",
                new="metrics_num_episodes_for_smoothing",
                error=False,
            )
            config["metrics_num_episodes_for_smoothing"] = config[
                "metrics_smoothing_episodes"
            ]
        if config.get("min_iter_time_s", DEPRECATED_VALUE) != DEPRECATED_VALUE:
            deprecation_warning(
                old="min_iter_time_s",
                new="min_time_s_per_reporting",
                error=False,
            )
            config["min_time_s_per_reporting"] = config["min_iter_time_s"] or 0

        if config.get("collect_metrics_timeout", DEPRECATED_VALUE) != DEPRECATED_VALUE:
            # TODO: Warn once all algos use the `training_iteration` method.
            # deprecation_warning(
            #     old="collect_metrics_timeout",
            #     new="metrics_episode_collection_timeout_s",
            #     error=False,
            # )
            config["metrics_episode_collection_timeout_s"] = config[
                "collect_metrics_timeout"
            ]

        if config.get("timesteps_per_iteration", DEPRECATED_VALUE) != DEPRECATED_VALUE:
            deprecation_warning(
                old="timesteps_per_iteration",
                new="`min_sample_timesteps_per_reporting` OR "
                "`min_train_timesteps_per_reporting`",
                error=False,
            )
            config["min_sample_timesteps_per_reporting"] = (
                config["timesteps_per_iteration"] or 0
            )
            config["timesteps_per_iteration"] = DEPRECATED_VALUE

        # Evaluation settings.

        # Deprecated setting: `evaluation_num_episodes`.
        if config.get("evaluation_num_episodes", DEPRECATED_VALUE) != DEPRECATED_VALUE:
            deprecation_warning(
                old="evaluation_num_episodes",
                new="`evaluation_duration` and `evaluation_duration_unit=episodes`",
                error=False,
            )
            config["evaluation_duration"] = config["evaluation_num_episodes"]
            config["evaluation_duration_unit"] = "episodes"
            config["evaluation_num_episodes"] = DEPRECATED_VALUE

        # If `evaluation_num_workers` > 0, warn if `evaluation_interval` is
        # None (also set `evaluation_interval` to 1).
        if config["evaluation_num_workers"] > 0 and not config["evaluation_interval"]:
            logger.warning(
                f"You have specified {config['evaluation_num_workers']} "
                "evaluation workers, but your `evaluation_interval` is None! "
                "Therefore, evaluation will not occur automatically with each"
                " call to `Trainer.train()`. Instead, you will have to call "
                "`Trainer.evaluate()` manually in order to trigger an "
                "evaluation run."
            )
        # If `evaluation_num_workers=0` and
        # `evaluation_parallel_to_training=True`, warn that you need
        # at least one remote eval worker for parallel training and
        # evaluation, and set `evaluation_parallel_to_training` to False.
        elif config["evaluation_num_workers"] == 0 and config.get(
            "evaluation_parallel_to_training", False
        ):
            logger.warning(
                "`evaluation_parallel_to_training` can only be done if "
                "`evaluation_num_workers` > 0! Setting "
                "`evaluation_parallel_to_training` to False."
            )
            config["evaluation_parallel_to_training"] = False

        # If `evaluation_duration=auto`, error if
        # `evaluation_parallel_to_training=False`.
        if config["evaluation_duration"] == "auto":
            if not config["evaluation_parallel_to_training"]:
                raise ValueError(
                    "`evaluation_duration=auto` not supported for "
                    "`evaluation_parallel_to_training=False`!"
                )
        # Make sure, it's an int otherwise.
        elif (
            not isinstance(config["evaluation_duration"], int)
            or config["evaluation_duration"] <= 0
        ):
            raise ValueError(
                "`evaluation_duration` ({}) must be an int and "
                ">0!".format(config["evaluation_duration"])
            )

    @staticmethod
    @ExperimentalAPI
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
        # Try to get our "main" WorkerSet (used for training sample collection).
        workers = getattr(self, "workers", None)
        if not isinstance(workers, WorkerSet):
            return

        removed_workers, new_workers = [], []
        # Search for failed workers and try to recover (restart) them.
        if self.config["recreate_failed_workers"] is True:
            removed_workers, new_workers = workers.recreate_failed_workers()
        elif self.config["ignore_worker_failures"] is True:
            removed_workers = workers.remove_failed_workers()
        self.on_worker_failures(removed_workers, new_workers)

        if not self.config.get("_disable_execution_plan_api") and callable(
            self.execution_plan
        ):
            logger.warning("Recreating execution plan after failure")
            self.train_exec_impl = self.execution_plan(
                workers, self.config, **self._kwargs_for_execution_plan()
            )

    def on_worker_failures(
        self, removed_workers: List[ActorHandle], new_workers: List[ActorHandle]
    ):
        """Called after a worker failure is detected.

        Args:
            removed_workers: List of removed workers.
            new_workers: List of new workers.
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

    def __getstate__(self) -> dict:
        state = {}
        if hasattr(self, "workers"):
            state["worker"] = self.workers.local_worker().save()
        # TODO: Experimental functionality: Store contents of replay buffer
        #  to checkpoint, only if user has configured this.
        if self.local_replay_buffer is not None and self.config.get(
            "store_buffer_in_checkpoints"
        ):
            state["local_replay_buffer"] = self.local_replay_buffer.get_state()

        if self.train_exec_impl is not None:
            state["train_exec_impl"] = self.train_exec_impl.shared_metrics.get().save()

        return state

    def __setstate__(self, state: dict):
        if hasattr(self, "workers") and "worker" in state:
            self.workers.local_worker().restore(state["worker"])
            remote_state = ray.put(state["worker"])
            for r in self.workers.remote_workers():
                r.restore.remote(remote_state)
            if self.evaluation_workers:
                # If evaluation workers are used, also restore the policies
                # there in case they are used for evaluation purpose.
                for r in self.evaluation_workers.remote_workers():
                    r.restore.remote(remote_state)
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

        if self.train_exec_impl is not None:
            self.train_exec_impl.shared_metrics.get().restore(state["train_exec_impl"])

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
        if not config.get("replay_buffer_config") or config["replay_buffer_config"].get(
            "no_local_replay_buffer" or config.get("no_local_replay_buffer")
        ):
            return

        buffer_type = config["replay_buffer_config"]["type"]
        return from_config(buffer_type, config["replay_buffer_config"])

    @DeveloperAPI
    def _kwargs_for_execution_plan(self):
        kwargs = {}
        if self.local_replay_buffer is not None:
            kwargs["local_replay_buffer"] = self.local_replay_buffer
        return kwargs

    def _step_context(trainer):
        class StepCtx:
            def __enter__(self):
                # Before first call to `step()`, `result` is expected to be None ->
                # Start with self.failures=-1 -> set to 0 before the very first call
                # to `self.step()`.
                self.failures = -1

                self.time_start = time.time()
                self.sampled = 0
                self.trained = 0
                self.init_env_steps_sampled = trainer._counters[NUM_ENV_STEPS_SAMPLED]
                self.init_env_steps_trained = trainer._counters[NUM_ENV_STEPS_TRAINED]
                self.init_agent_steps_sampled = trainer._counters[
                    NUM_AGENT_STEPS_SAMPLED
                ]
                self.init_agent_steps_trained = trainer._counters[
                    NUM_AGENT_STEPS_TRAINED
                ]
                self.failure_tolerance = trainer.config[
                    "num_consecutive_worker_failures_tolerance"
                ]
                return self

            def __exit__(self, *args):
                pass

            def should_stop(self, result):

                # Before first call to `step()`, `result` is expected to be None ->
                # self.failures=0.
                if result is None:
                    # Fail after n retries.
                    self.failures += 1
                    if self.failures > self.failure_tolerance:
                        raise RuntimeError(
                            "More than `num_consecutive_worker_failures_tolerance="
                            f"{self.failure_tolerance}` consecutive worker failures! "
                            "Exiting."
                        )
                    # Continue to very first `step()` call or retry `step()` after
                    # a (tolerable) failure.
                    return False

                # Stopping criteria: Only when using the `training_iteration`
                # API, b/c for the `exec_plan` API, the logic to stop is
                # already built into the execution plans via the
                # `StandardMetricsReporting` op.
                elif trainer.config["_disable_execution_plan_api"]:
                    if trainer._by_agent_steps:
                        self.sampled = (
                            trainer._counters[NUM_AGENT_STEPS_SAMPLED]
                            - self.init_agent_steps_sampled
                        )
                        self.trained = (
                            trainer._counters[NUM_AGENT_STEPS_TRAINED]
                            - self.init_agent_steps_trained
                        )
                    else:
                        self.sampled = (
                            trainer._counters[NUM_ENV_STEPS_SAMPLED]
                            - self.init_env_steps_sampled
                        )
                        self.trained = (
                            trainer._counters[NUM_ENV_STEPS_TRAINED]
                            - self.init_env_steps_trained
                        )

                    min_t = trainer.config["min_time_s_per_reporting"]
                    min_sample_ts = trainer.config["min_sample_timesteps_per_reporting"]
                    min_train_ts = trainer.config["min_train_timesteps_per_reporting"]
                    # Repeat if not enough time has passed or if not enough
                    # env|train timesteps have been processed (or these min
                    # values are not provided by the user).
                    if (
                        result is not None
                        and (not min_t or time.time() - self.time_start >= min_t)
                        and (not min_sample_ts or self.sampled >= min_sample_ts)
                        and (not min_train_ts or self.trained >= min_train_ts)
                    ):
                        return True
                    else:
                        return False
                # No errors (we got results != None) -> Return True
                # (meaning: yes, should stop -> no further step attempts).
                else:
                    return True

        return StepCtx()

    def _compile_step_results(self, *, step_ctx, step_attempt_results=None):
        # Return dict.
        results: ResultDict = {}
        step_attempt_results = step_attempt_results or {}

        # Evaluation results.
        if "evaluation" in step_attempt_results:
            results["evaluation"] = step_attempt_results.pop("evaluation")

        # Custom metrics and episode media.
        results["custom_metrics"] = step_attempt_results.pop("custom_metrics", {})
        results["episode_media"] = step_attempt_results.pop("episode_media", {})

        # Learner info.
        results["info"] = {LEARNER_INFO: step_attempt_results}

        episodes = self._rollout_worker_metrics
        orig_episodes = list(episodes)
        missing = self.config["metrics_num_episodes_for_smoothing"] - len(episodes)
        if missing > 0:
            episodes = self._episode_history[-missing:] + episodes
            assert len(episodes) <= self.config["metrics_num_episodes_for_smoothing"]
        self._episode_history.extend(orig_episodes)
        self._episode_history = self._episode_history[
            -self.config["metrics_num_episodes_for_smoothing"] :
        ]
        results["sampler_results"] = summarize_episodes(
            episodes, orig_episodes, self.config["keep_per_episode_custom_metrics"]
        )
        # TODO: Don't dump sampler results into top-level.
        results.update(results["sampler_results"])

        results["num_healthy_workers"] = len(self.workers.remote_workers())

        # Train-steps- and env/agent-steps this iteration.
        for c in [
            NUM_AGENT_STEPS_SAMPLED,
            NUM_AGENT_STEPS_TRAINED,
            NUM_ENV_STEPS_SAMPLED,
            NUM_ENV_STEPS_TRAINED,
        ]:
            results[c] = self._counters[c]
        if self._by_agent_steps:
            results[NUM_AGENT_STEPS_SAMPLED + "_this_iter"] = step_ctx.sampled
            results[NUM_AGENT_STEPS_TRAINED + "_this_iter"] = step_ctx.trained
            # TODO: For CQL and other algos, count by trained steps.
            results["timesteps_total"] = self._counters[NUM_AGENT_STEPS_SAMPLED]
        else:
            results[NUM_ENV_STEPS_SAMPLED + "_this_iter"] = step_ctx.sampled
            results[NUM_ENV_STEPS_TRAINED + "_this_iter"] = step_ctx.trained
            # TODO: For CQL and other algos, count by trained steps.
            results["timesteps_total"] = self._counters[NUM_ENV_STEPS_SAMPLED]
        # TODO: Backward compatibility.
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

    @Deprecated(new="Trainer.compute_single_action()", error=False)
    def compute_action(self, *args, **kwargs):
        return self.compute_single_action(*args, **kwargs)

    @Deprecated(new="construct WorkerSet(...) instance directly", error=False)
    def _make_workers(
        self,
        *,
        env_creator: EnvCreator,
        validate_env: Optional[Callable[[EnvType, EnvContext], None]],
        policy_class: Type[Policy],
        config: TrainerConfigDict,
        num_workers: int,
        local_worker: bool = True,
    ) -> WorkerSet:
        return WorkerSet(
            env_creator=env_creator,
            validate_env=validate_env,
            policy_class=policy_class,
            trainer_config=config,
            num_workers=num_workers,
            local_worker=local_worker,
            logdir=self.logdir,
        )

    @Deprecated(new="Trainer.try_recover_from_step_attempt()", error=False)
    def _try_recover(self):
        return self.try_recover_from_step_attempt()

    @staticmethod
    @Deprecated(new="Trainer.validate_config()", error=False)
    def _validate_config(config, trainer_or_none):
        assert trainer_or_none is not None
        return trainer_or_none.validate_config(config)


# TODO: Create a dict that throw a deprecation warning once we have fully moved
#  to TrainerConfig() objects (some algos still missing).
COMMON_CONFIG: TrainerConfigDict = TrainerConfig(Trainer).to_dict()

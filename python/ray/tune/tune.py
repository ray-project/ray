import abc
import copy
import datetime
import logging
import os
import signal
import sys
import threading
import time
import warnings
from typing import (
    Any,
    Callable,
    Dict,
    Mapping,
    Optional,
    Sequence,
    Type,
    Union,
    TYPE_CHECKING,
)

import ray
from ray.air._internal import usage as air_usage
from ray.air._internal.usage import AirEntrypoint
from ray.air.util.node import _force_on_current_node
from ray.train import CheckpointConfig, SyncConfig
from ray.train.constants import RAY_CHDIR_TO_TRIAL_DIR, _DEPRECATED_VALUE
from ray.tune.analysis import ExperimentAnalysis
from ray.tune.callback import Callback
from ray.tune.error import TuneError
from ray.tune.execution.tune_controller import TuneController
from ray.tune.experiment import Experiment, _convert_to_experiment_list
from ray.tune.experimental.output import (
    get_air_verbosity,
    IS_NOTEBOOK,
    AirVerbosity,
)

from ray.tune.impl.placeholder import create_resolvers_map, inject_placeholders
from ray.tune.logger import TBXLoggerCallback
from ray.tune.progress_reporter import (
    ProgressReporter,
    _detect_reporter,
    _detect_progress_metrics,
    _prepare_progress_reporter_for_ray_client,
    _stream_client_output,
)
from ray.tune.registry import get_trainable_cls, is_function_trainable

# Must come last to avoid circular imports
from ray.tune.schedulers import (
    FIFOScheduler,
    PopulationBasedTraining,
    PopulationBasedTrainingReplay,
    ResourceChangingScheduler,
    TrialScheduler,
)
from ray.tune.schedulers.util import (
    _set_search_properties_backwards_compatible as scheduler_set_search_props,
)
from ray.tune.stopper import Stopper
from ray.tune.search import (
    BasicVariantGenerator,
    SearchAlgorithm,
    SearchGenerator,
    ConcurrencyLimiter,
    Searcher,
    create_searcher,
)
from ray.tune.search.util import (
    _set_search_properties_backwards_compatible as searcher_set_search_props,
)
from ray.tune.search.variant_generator import _has_unresolved_values
from ray.tune.trainable import Trainable
from ray.tune.experiment import Trial
from ray.tune.utils.callback import _create_default_callbacks
from ray.tune.utils.log import (
    Verbosity,
    has_verbosity,
    set_verbosity,
)
from ray.tune.execution.placement_groups import PlacementGroupFactory
from ray.util.annotations import PublicAPI
from ray.util.queue import Queue

if TYPE_CHECKING:
    import pyarrow.fs

    from ray.tune.experimental.output import ProgressReporter as AirProgressReporter

logger = logging.getLogger(__name__)


def _get_trainable(
    run_identifier: Union[Experiment, str, Type, Callable]
) -> Optional[Type[Trainable]]:
    if isinstance(run_identifier, Experiment):
        run_identifier = run_identifier.run_identifier

    if isinstance(run_identifier, type):
        if not issubclass(run_identifier, Trainable):
            # If obscure dtype, assume it is overridden.
            return None
        trainable_cls = run_identifier
    elif callable(run_identifier):
        trainable_cls = run_identifier
    elif isinstance(run_identifier, str):
        trainable_cls = get_trainable_cls(run_identifier)
    else:
        return None

    return trainable_cls


def _check_default_resources_override(
    run_identifier: Union[Experiment, str, Type, Callable]
) -> bool:
    trainable_cls = _get_trainable(run_identifier)
    if not trainable_cls:
        # If no trainable, assume override
        return True

    return hasattr(trainable_cls, "default_resource_request") and (
        trainable_cls.default_resource_request.__code__
        != Trainable.default_resource_request.__code__
    )


def _check_mixin(run_identifier: Union[Experiment, str, Type, Callable]) -> bool:
    trainable_cls = _get_trainable(run_identifier)
    if not trainable_cls:
        # Default to True
        return True

    return hasattr(trainable_cls, "__mixins__") or getattr(
        trainable_cls, "_is_mixin", False
    )


def _check_gpus_in_resources(
    resources: Optional[Union[Dict, PlacementGroupFactory]]
) -> bool:
    if not resources:
        return False

    if isinstance(resources, PlacementGroupFactory):
        return bool(resources.required_resources.get("GPU", None))

    if isinstance(resources, dict):
        return bool(resources.get("gpu", None))


def _report_progress(
    runner: TuneController, reporter: ProgressReporter, done: bool = False
):
    """Reports experiment progress.

    Args:
        runner: Trial runner to report on.
        reporter: Progress reporter.
        done: Whether this is the last progress report attempt.
    """
    trials = runner.get_trials()
    if reporter.should_report(trials, done=done):
        sched_debug_str = runner.scheduler_alg.debug_string()
        used_resources_str = runner._used_resources_string()
        reporter.report(trials, done, sched_debug_str, used_resources_str)


def _report_air_progress(
    runner: TuneController, reporter: "AirProgressReporter", force: bool = False
):
    trials = runner.get_trials()
    reporter_args = []
    used_resources_string = runner._used_resources_string()
    reporter_args.append(used_resources_string)
    reporter.print_heartbeat(trials, *reporter_args, force=force)


def _setup_signal_catching() -> threading.Event:
    original_handler = signal.getsignal(signal.SIGINT)
    experiment_interrupted_event = threading.Event()

    def signal_interrupt_tune_run(sig: int, frame):
        logger.warning(
            "Stop signal received (e.g. via SIGINT/Ctrl+C), ending Ray Tune run. "
            "This will try to checkpoint the experiment state one last time. "
            "Press CTRL+C (or send SIGINT/SIGKILL/SIGTERM) "
            "to skip. "
        )
        experiment_interrupted_event.set()
        # Restore original signal handler to react to future SIGINT signals.
        signal.signal(signal.SIGINT, original_handler)

    # We should only install the handler when it is safe to do so.
    # When tune.run() is called from worker thread, signal.signal will
    # fail.
    allow_signal_catching = True
    if threading.current_thread() != threading.main_thread():
        allow_signal_catching = False

    if allow_signal_catching:
        if not int(os.getenv("TUNE_DISABLE_SIGINT_HANDLER", "0")):
            signal.signal(signal.SIGINT, signal_interrupt_tune_run)

        # Always register SIGUSR1 if available (not available e.g. on Windows)
        if hasattr(signal, "SIGUSR1"):
            signal.signal(signal.SIGUSR1, signal_interrupt_tune_run)

    return experiment_interrupted_event


def _ray_auto_init(entrypoint: str):
    """Initialize Ray unless already configured."""
    if os.environ.get("TUNE_DISABLE_AUTO_INIT") == "1":
        logger.info("'TUNE_DISABLE_AUTO_INIT=1' detected.")
    elif not ray.is_initialized():
        ray.init()
        logger.info(
            "Initializing Ray automatically. "
            "For cluster usage or custom Ray initialization, "
            f"call `ray.init(...)` before `{entrypoint}`."
        )


class _Config(abc.ABC):
    def to_dict(self) -> dict:
        """Converts this configuration to a dict format."""
        raise NotImplementedError


@PublicAPI
def run(
    run_or_experiment: Union[str, Callable, Type],
    *,
    name: Optional[str] = None,
    metric: Optional[str] = None,
    mode: Optional[str] = None,
    stop: Optional[Union[Mapping, Stopper, Callable[[str, Mapping], bool]]] = None,
    time_budget_s: Optional[Union[int, float, datetime.timedelta]] = None,
    config: Optional[Dict[str, Any]] = None,
    resources_per_trial: Union[
        None, Mapping[str, Union[float, int, Mapping]], PlacementGroupFactory
    ] = None,
    num_samples: int = 1,
    storage_path: Optional[str] = None,
    storage_filesystem: Optional["pyarrow.fs.FileSystem"] = None,
    search_alg: Optional[Union[Searcher, SearchAlgorithm, str]] = None,
    scheduler: Optional[Union[TrialScheduler, str]] = None,
    checkpoint_config: Optional[CheckpointConfig] = None,
    verbose: Optional[Union[int, AirVerbosity, Verbosity]] = None,
    progress_reporter: Optional[ProgressReporter] = None,
    log_to_file: bool = False,
    trial_name_creator: Optional[Callable[[Trial], str]] = None,
    trial_dirname_creator: Optional[Callable[[Trial], str]] = None,
    sync_config: Optional[SyncConfig] = None,
    export_formats: Optional[Sequence] = None,
    max_failures: int = 0,
    fail_fast: bool = False,
    restore: Optional[str] = None,
    server_port: Optional[int] = None,
    resume: Union[bool, str] = False,
    reuse_actors: Optional[bool] = None,
    raise_on_failed_trial: bool = True,
    callbacks: Optional[Sequence[Callback]] = None,
    max_concurrent_trials: Optional[int] = None,
    # Deprecated
    keep_checkpoints_num: Optional[int] = None,  # Deprecated (2.7)
    checkpoint_score_attr: Optional[str] = None,  # Deprecated (2.7)
    checkpoint_freq: int = 0,  # Deprecated (2.7)
    checkpoint_at_end: bool = False,  # Deprecated (2.7)
    chdir_to_trial_dir: bool = _DEPRECATED_VALUE,  # Deprecated (2.8)
    local_dir: Optional[str] = None,
    # == internal only ==
    _experiment_checkpoint_dir: Optional[str] = None,
    _remote: Optional[bool] = None,
    # Passed by the Tuner.
    _remote_string_queue: Optional[Queue] = None,
    # Todo (krfricke): Find a better way to pass entrypoint information, e.g.
    # a context object or similar.
    _entrypoint: AirEntrypoint = AirEntrypoint.TUNE_RUN,
) -> ExperimentAnalysis:
    """Executes training.

    When a SIGINT signal is received (e.g. through Ctrl+C), the tuning run
    will gracefully shut down and checkpoint the latest experiment state.
    Sending SIGINT again (or SIGKILL/SIGTERM instead) will skip this step.

    Many aspects of Tune, such as the frequency of global checkpointing,
    maximum pending placement group trials and the path of the result
    directory be configured through environment variables. Refer to
    :ref:`tune-env-vars` for a list of environment variables available.

    Examples:

    .. code-block:: python

        # Run 10 trials (each trial is one instance of a Trainable). Tune runs
        # in parallel and automatically determines concurrency.
        tune.run(trainable, num_samples=10)

        # Run 1 trial, stop when trial has reached 10 iterations
        tune.run(my_trainable, stop={"training_iteration": 10})

        # automatically retry failed trials up to 3 times
        tune.run(my_trainable, stop={"training_iteration": 10}, max_failures=3)

        # Run 1 trial, search over hyperparameters, stop after 10 iterations.
        space = {"lr": tune.uniform(0, 1), "momentum": tune.uniform(0, 1)}
        tune.run(my_trainable, config=space, stop={"training_iteration": 10})

        # Resumes training if a previous machine crashed
        tune.run(my_trainable, config=space,
                 local_dir=<path/to/dir>, resume=True)

        # Rerun ONLY failed trials after an experiment is finished.
        tune.run(my_trainable, config=space,
                 local_dir=<path/to/dir>, resume="ERRORED_ONLY")

    Args:
        run_or_experiment: If function|class|str, this is the algorithm or
            model to train. This may refer to the name of a built-on algorithm
            (e.g. RLlib's DQN or PPO), a user-defined trainable
            function or class, or the string identifier of a
            trainable function or class registered in the tune registry.
            If Experiment, then Tune will execute training based on
            Experiment.spec. If you want to pass in a Python lambda, you
            will need to first register the function:
            ``tune.register_trainable("lambda_id", lambda x: ...)``. You can
            then use ``tune.run("lambda_id")``.
        metric: Metric to optimize. This metric should be reported
            with `tune.report()`. If set, will be passed to the search
            algorithm and scheduler.
        mode: Must be one of [min, max]. Determines whether objective is
            minimizing or maximizing the metric attribute. If set, will be
            passed to the search algorithm and scheduler.
        name: Name of experiment.
        stop: Stopping criteria. If dict,
            the keys may be any field in the return result of 'train()',
            whichever is reached first. If function, it must take (trial_id,
            result) as arguments and return a boolean (True if trial should be
            stopped, False otherwise). This can also be a subclass of
            ``ray.tune.Stopper``, which allows users to implement
            custom experiment-wide stopping (i.e., stopping an entire Tune
            run based on some time constraint).
        time_budget_s: Global time budget in
            seconds after which all trials are stopped. Can also be a
            ``datetime.timedelta`` object.
        config: Algorithm-specific configuration for Tune variant
            generation (e.g. env, hyperparams). Defaults to empty dict.
            Custom search algorithms may ignore this.
        resources_per_trial: Machine resources
            to allocate per trial, e.g. ``{"cpu": 64, "gpu": 8}``.
            Note that GPUs will not be assigned unless you specify them here.
            Defaults to 1 CPU and 0 GPUs in
            ``Trainable.default_resource_request()``. This can also
            be a PlacementGroupFactory object wrapping arguments to create a
            per-trial placement group.
        num_samples: Number of times to sample from the
            hyperparameter space. Defaults to 1. If `grid_search` is
            provided as an argument, the grid will be repeated
            `num_samples` of times. If this is -1, (virtually) infinite
            samples are generated until a stopping condition is met.
        storage_path: Path to store results at. Can be a local directory or
            a destination on cloud storage. Defaults to
            the local ``~/ray_results`` directory.
        search_alg: Search algorithm for
            optimization. You can also use the name of the algorithm.
        scheduler: Scheduler for executing
            the experiment. Choose among FIFO (default), MedianStopping,
            AsyncHyperBand, HyperBand and PopulationBasedTraining. Refer to
            ray.tune.schedulers for more options. You can also use the
            name of the scheduler.
        verbose: 0, 1, or 2. Verbosity mode.
            0 = silent, 1 = default, 2 = verbose. Defaults to 1.
            If the ``RAY_AIR_NEW_OUTPUT=1`` environment variable is set,
            uses the old verbosity settings:
            0 = silent, 1 = only status updates, 2 = status and brief
            results, 3 = status and detailed results.
        progress_reporter: Progress reporter for reporting
            intermediate experiment progress. Defaults to CLIReporter if
            running in command-line, or JupyterNotebookReporter if running in
            a Jupyter notebook.
        log_to_file: Log stdout and stderr to files in
            Tune's trial directories. If this is `False` (default), no files
            are written. If `true`, outputs are written to `trialdir/stdout`
            and `trialdir/stderr`, respectively. If this is a single string,
            this is interpreted as a file relative to the trialdir, to which
            both streams are written. If this is a Sequence (e.g. a Tuple),
            it has to have length 2 and the elements indicate the files to
            which stdout and stderr are written, respectively.
        trial_name_creator: Optional function that takes in a Trial and returns
            its name (i.e. its string representation). Be sure to include some unique
            identifier (such as `Trial.trial_id`) in each trial's name.
        trial_dirname_creator: Optional function that takes in a trial and
            generates its trial directory name as a string. Be sure to include some
            unique identifier (such as `Trial.trial_id`) is used in each trial's
            directory name. Otherwise, trials could overwrite artifacts and checkpoints
            of other trials. The return value cannot be a path.
        chdir_to_trial_dir: Deprecated. Use `RAY_CHDIR_TO_TRIAL_DIR=0` instead.
            Whether to change the working directory of each worker
            to its corresponding trial directory. Defaults to `True` to prevent
            contention between workers saving trial-level outputs.
            If set to `False`, files are accessible with paths relative to the
            original working directory. However, all workers on the same node now
            share the same working directory, so be sure to use
            `ray.train.get_context().get_trial_dir()` as the path to save any outputs.
        sync_config: Configuration object for syncing. See train.SyncConfig.
        export_formats: List of formats that exported at the end of
            the experiment. Default is None.
        max_failures: Try to recover a trial at least this many times.
            Ray will recover from the latest checkpoint if present.
            Setting to -1 will lead to infinite recovery retries.
            Setting to 0 will disable retries. Defaults to 0.
        fail_fast: Whether to fail upon the first error.
            If fail_fast='raise' provided, Tune will automatically
            raise the exception received by the Trainable. fail_fast='raise'
            can easily leak resources and should be used with caution (it
            is best used with `ray.init(local_mode=True)`).
        restore: Path to checkpoint. Only makes sense to set if
            running 1 trial. Defaults to None.
        server_port: Port number for launching TuneServer.
        resume: One of [True, False, "LOCAL", "REMOTE", "PROMPT", "AUTO"]. Can
            be suffixed with one or more of ["+ERRORED", "+ERRORED_ONLY",
            "+RESTART_ERRORED", "+RESTART_ERRORED_ONLY"] (e.g. ``AUTO+ERRORED``).
            "LOCAL"/True restores the checkpoint from the
            local experiment directory, determined
            by ``name`` and ``local_dir``.
            "REMOTE" restores the checkpoint
            from ``upload_dir`` (as passed to ``sync_config``).
            "PROMPT" provides the CLI feedback.
            False forces a new experiment.
            "AUTO" will attempt to resume from a checkpoint and otherwise
            start a new experiment.
            The suffix "+ERRORED" resets and reruns errored trials upon resume -
            previous trial artifacts will be left untouched. It will try to continue
            from the last observed checkpoint.
            The suffix "+RESTART_ERRORED" will instead start the errored trials from
            scratch. "+ERRORED_ONLY" and "+RESTART_ERRORED_ONLY" will disable
            resuming non-errored trials - they will be added as finished instead. New
            trials can still be generated by the search algorithm.
            If resume is set but checkpoint does not exist,
            ValueError will be thrown.
        reuse_actors: Whether to reuse actors between different trials
            when possible. This can drastically speed up experiments that start
            and stop actors often (e.g., PBT in time-multiplexing mode). This
            requires trials to have the same resource requirements.
            Defaults to ``True`` for function trainables and ``False`` for
            class and registered trainables.
        raise_on_failed_trial: Raise TuneError if there exists failed
            trial (of ERROR state) when the experiments complete.
        callbacks: List of callbacks that will be called at different
            times in the training loop. Must be instances of the
            ``ray.tune.callback.Callback`` class. If not passed,
            `LoggerCallback` (json/csv/tensorboard) callbacks are automatically added.
        max_concurrent_trials: Maximum number of trials to run
            concurrently. Must be non-negative. If None or 0, no limit will
            be applied. This is achieved by wrapping the ``search_alg`` in
            a :class:`ConcurrencyLimiter`, and thus setting this argument
            will raise an exception if the ``search_alg`` is already a
            :class:`ConcurrencyLimiter`. Defaults to None.
        _remote: Whether to run the Tune driver in a remote function.
            This is disabled automatically if a custom trial executor is
            passed in. This is enabled by default in Ray client mode.
        keep_checkpoints_num: Deprecated. use checkpoint_config instead.
        checkpoint_score_attr: Deprecated. use checkpoint_config instead.
        checkpoint_freq: Deprecated. use checkpoint_config instead.
        checkpoint_at_end: Deprecated. use checkpoint_config instead.
        checkpoint_keep_all_ranks: Deprecated. use checkpoint_config instead.
        checkpoint_upload_from_workers: Deprecated. use checkpoint_config instead.

    Returns:
        ExperimentAnalysis: Object for experiment analysis.

    Raises:
        TuneError: Any trials failed and `raise_on_failed_trial` is True.
    """
    # NO CODE IS TO BE ADDED ABOVE THIS COMMENT
    # remote_run_kwargs must be defined before any other
    # code is ran to ensure that at this point,
    # `locals()` is equal to args and kwargs
    remote_run_kwargs = locals().copy()
    remote_run_kwargs.pop("_remote")

    if _entrypoint == AirEntrypoint.TRAINER:
        error_message_map = {
            "entrypoint": "Trainer(...)",
            "search_space_arg": "param_space",
            "restore_entrypoint": 'Trainer.restore(path="{path}", ...)',
        }
    elif _entrypoint == AirEntrypoint.TUNER:
        error_message_map = {
            "entrypoint": "Tuner(...)",
            "search_space_arg": "param_space",
            "restore_entrypoint": 'Tuner.restore(path="{path}", trainable=...)',
        }
    elif _entrypoint == AirEntrypoint.TUNE_RUN_EXPERIMENTS:
        error_message_map = {
            "entrypoint": "tune.run_experiments(...)",
            "search_space_arg": "experiment=Experiment(config)",
            "restore_entrypoint": "tune.run_experiments(..., resume=True)",
        }
    else:
        error_message_map = {
            "entrypoint": "tune.run(...)",
            "search_space_arg": "config",
            "restore_entrypoint": "tune.run(..., resume=True)",
        }

    _ray_auto_init(entrypoint=error_message_map["entrypoint"])

    if _remote is None:
        _remote = ray.util.client.ray.is_connected()

    if verbose is None:
        # Default `verbose` value. For new output engine, this is AirVerbosity.VERBOSE.
        # For old output engine, this is Verbosity.V3_TRIAL_DETAILS
        verbose = get_air_verbosity(AirVerbosity.VERBOSE) or Verbosity.V3_TRIAL_DETAILS

    if _remote:
        if get_air_verbosity(verbose) is not None:
            logger.info(
                "[output] This uses the legacy output and progress reporter, "
                "as Ray client is not supported by the new engine. "
                "For more information, see "
                "https://github.com/ray-project/ray/issues/36949"
            )

        remote_run = ray.remote(num_cpus=0)(run)

        # Make sure tune.run is called on the sever node.
        remote_run = _force_on_current_node(remote_run)

        progress_reporter, string_queue = _prepare_progress_reporter_for_ray_client(
            progress_reporter, verbose, _remote_string_queue
        )

        # Override with detected progress reporter
        remote_run_kwargs["progress_reporter"] = progress_reporter

        remote_future = remote_run.remote(_remote=False, **remote_run_kwargs)

        _stream_client_output(
            remote_future,
            progress_reporter,
            string_queue,
        )
        return ray.get(remote_future)

    del remote_run_kwargs

    if os.environ.get("TUNE_RESULT_DIR"):
        # Deprecate: Raise in 2.6, remove in 2.7
        warnings.warn(
            "The TUNE_RESULT_DIR environment variable is deprecated and will be "
            "removed in the future. If you want to set persistent storage to "
            "a local directory, pass `storage_path` instead. If you are using "
            "remote storage and want to control the local cache directory, "
            "set the RAY_AIR_LOCAL_CACHE_DIR environment variable instead.",
            DeprecationWarning,
        )

    ray._private.usage.usage_lib.record_library_usage("tune")

    # Tracking environment variable usage here will also catch:
    # 1.) Tuner.fit() usage
    # 2.) Trainer.fit() usage
    # 3.) Ray client usage (env variables are inherited by the Ray runtime env)
    air_usage.tag_ray_air_env_vars()

    # Track the entrypoint to AIR:
    # Tuner.fit / Trainer.fit / tune.run / tune.run_experiments
    air_usage.tag_air_entrypoint(_entrypoint)

    all_start = time.time()

    if mode and mode not in ["min", "max"]:
        raise ValueError(
            f"The `mode` parameter passed to `{error_message_map['entrypoint']}` "
            "must be one of ['min', 'max']"
        )

    air_verbosity = get_air_verbosity(verbose)
    if air_verbosity is not None and IS_NOTEBOOK:
        logger.info(
            "[output] This uses the legacy output and progress reporter, "
            "as Jupyter notebooks are not supported by the new engine, yet. "
            "For more information, please see "
            "https://github.com/ray-project/ray/issues/36949"
        )
        air_verbosity = None

    if air_verbosity is not None:
        logger.info(
            f"[output] This will use the new output engine with verbosity "
            f"{air_verbosity}. To disable the new output and use the legacy "
            f"output engine, set the environment variable RAY_AIR_NEW_OUTPUT=0. "
            f"For more information, please see "
            f"https://github.com/ray-project/ray/issues/36949"
        )
        # Disable old output engine
        set_verbosity(0)
    else:
        # Use old output engine
        set_verbosity(verbose)

    config = config or {}
    if isinstance(config, _Config):
        config = config.to_dict()
    if not isinstance(config, dict):
        raise ValueError(
            f"The `{error_message_map['search_space_arg']}` passed to "
            f"`{error_message_map['entrypoint']}` must be a dict. "
            f"Got '{type(config)}' instead."
        )

    sync_config = sync_config or SyncConfig()

    # TODO(justinvyu): Finalize the local_dir vs. env var API in 2.8.
    # For now, keep accepting both options.
    if local_dir is not None:
        os.environ["RAY_AIR_LOCAL_CACHE_DIR"] = local_dir

    checkpoint_config = checkpoint_config or CheckpointConfig()

    # For backward compatibility
    # TODO(jungong): remove after 2.7 release.
    if keep_checkpoints_num is not None:
        warnings.warn(
            "keep_checkpoints_num is deprecated and will be removed. "
            "use checkpoint_config.num_to_keep instead.",
            DeprecationWarning,
        )
        checkpoint_config.num_to_keep = keep_checkpoints_num
    if checkpoint_score_attr is not None:
        warnings.warn(
            "checkpoint_score_attr is deprecated and will be removed. "
            "use checkpoint_config.checkpoint_score_attribute instead.",
            DeprecationWarning,
        )

        if checkpoint_score_attr.startswith("min-"):
            warnings.warn(
                "using min- and max- prefixes to specify checkpoint score "
                "order is deprecated. Use CheckpointConfig.checkpoint_score_order "
                "instead",
                DeprecationWarning,
            )
            checkpoint_config.checkpoint_score_attribute = checkpoint_score_attr[4:]
            checkpoint_config.checkpoint_score_order = "min"
        else:
            checkpoint_config.checkpoint_score_attribute = checkpoint_score_attr
            checkpoint_config.checkpoint_score_order = "max"

        checkpoint_config.score_attr = checkpoint_score_attr
    if checkpoint_freq > 0:
        warnings.warn(
            "checkpoint_freq is deprecated and will be removed. "
            "use checkpoint_config.checkpoint_frequency instead.",
            DeprecationWarning,
        )
        checkpoint_config.checkpoint_frequency = checkpoint_freq
    if checkpoint_at_end:
        warnings.warn(
            "checkpoint_at_end is deprecated and will be removed. "
            "use checkpoint_config.checkpoint_at_end instead.",
            DeprecationWarning,
        )
        checkpoint_config.checkpoint_at_end = checkpoint_at_end

    if chdir_to_trial_dir != _DEPRECATED_VALUE:
        warnings.warn(
            "`chdir_to_trial_dir` is deprecated and will be removed. "
            f"Use the {RAY_CHDIR_TO_TRIAL_DIR} environment variable instead. "
            "Set it to 0 to disable the default behavior of changing the "
            "working directory.",
            DeprecationWarning,
        )
        if chdir_to_trial_dir is False:
            os.environ[RAY_CHDIR_TO_TRIAL_DIR] = "0"

    if num_samples == -1:
        num_samples = sys.maxsize

    # Create scheduler here as we need access to some of its properties
    if isinstance(scheduler, str):
        # importing at top level causes a recursive dependency
        from ray.tune.schedulers import create_scheduler

        scheduler = create_scheduler(scheduler)
    scheduler = scheduler or FIFOScheduler()

    if not scheduler.supports_buffered_results:
        # Result buffering with e.g. a Hyperband scheduler is a bad idea, as
        # hyperband tries to stop trials when processing brackets. With result
        # buffering, we might trigger this multiple times when evaluating
        # a single trial, which leads to unexpected behavior.
        env_result_buffer_length = os.getenv("TUNE_RESULT_BUFFER_LENGTH", "")
        if env_result_buffer_length:
            warnings.warn(
                f"You are using a {type(scheduler)} scheduler, but "
                f"TUNE_RESULT_BUFFER_LENGTH is set "
                f"({env_result_buffer_length}). This can lead to undesired "
                f"and faulty behavior, so the buffer length was forcibly set "
                f"to 1 instead."
            )
            os.environ["TUNE_RESULT_BUFFER_LENGTH"] = "1"

    # If reuse_actors is unset, default to False for string and class trainables,
    # and default to True for everything else (i.e. function trainables)
    if reuse_actors is None:
        trainable = (
            run_or_experiment.run_identifier
            if isinstance(run_or_experiment, Experiment)
            else run_or_experiment
        )
        reuse_actors = (
            # Only default to True for function trainables that meet certain conditions
            is_function_trainable(trainable)
            and not (
                # Changing resources requires restarting actors
                scheduler
                and isinstance(scheduler, ResourceChangingScheduler)
            )
            and not (
                # If GPUs are requested we could run into problems with device memory
                _check_gpus_in_resources(resources_per_trial)
            )
            and not (
                # If the resource request is overridden, we don't know if GPUs
                # will be requested, yet, so default to False
                _check_default_resources_override(trainable)
            )
            and not (
                # Mixins do not work with reuse_actors as the mixin setup will only
                # be invoked once
                _check_mixin(trainable)
            )
        )
        logger.debug(f"Auto-detected `reuse_actors={reuse_actors}`")

    if (
        isinstance(scheduler, (PopulationBasedTraining, PopulationBasedTrainingReplay))
        and not reuse_actors
    ):
        warnings.warn(
            "Consider boosting PBT performance by enabling `reuse_actors` as "
            "well as implementing `reset_config` for Trainable."
        )

    # Before experiments are created, we first clean up the passed in
    # Config dictionary by replacing all the non-primitive config values
    # with placeholders. This serves two purposes:
    # 1. we can replace and "fix" these objects if a Trial is restored.
    # 2. the config dictionary will then be compatible with all supported
    #   search algorithms, since a lot of them do not support non-primitive
    #   config values.
    placeholder_resolvers = create_resolvers_map()
    config = inject_placeholders(
        # Make a deep copy here to avoid modifying the original config dict.
        copy.deepcopy(config),
        placeholder_resolvers,
    )

    # TODO(justinvyu): We should remove the ability to pass a list of
    # trainables to tune.run.
    if isinstance(run_or_experiment, list):
        experiments = run_or_experiment
    else:
        experiments = [run_or_experiment]

    for i, exp in enumerate(experiments):
        if not isinstance(exp, Experiment):
            experiments[i] = Experiment(
                name=name,
                run=exp,
                stop=stop,
                time_budget_s=time_budget_s,
                config=config,
                resources_per_trial=resources_per_trial,
                num_samples=num_samples,
                storage_path=storage_path,
                storage_filesystem=storage_filesystem,
                _experiment_checkpoint_dir=_experiment_checkpoint_dir,
                sync_config=sync_config,
                checkpoint_config=checkpoint_config,
                trial_name_creator=trial_name_creator,
                trial_dirname_creator=trial_dirname_creator,
                log_to_file=log_to_file,
                export_formats=export_formats,
                max_failures=max_failures,
                restore=restore,
            )

    if fail_fast and max_failures != 0:
        raise ValueError("max_failures must be 0 if fail_fast=True.")

    if isinstance(search_alg, str):
        search_alg = create_searcher(search_alg)

    # if local_mode=True is set during ray.init().
    is_local_mode = ray._private.worker._mode() == ray._private.worker.LOCAL_MODE

    if is_local_mode:
        max_concurrent_trials = 1

    if not search_alg:
        search_alg = BasicVariantGenerator(max_concurrent=max_concurrent_trials or 0)
    elif max_concurrent_trials or is_local_mode:
        if isinstance(search_alg, ConcurrencyLimiter):
            if not is_local_mode:
                if search_alg.max_concurrent != max_concurrent_trials:
                    raise ValueError(
                        "You have specified `max_concurrent_trials="
                        f"{max_concurrent_trials}`, but the `search_alg` is "
                        "already a `ConcurrencyLimiter` with `max_concurrent="
                        f"{search_alg.max_concurrent}. FIX THIS by setting "
                        "`max_concurrent_trials=None`."
                    )
                else:
                    logger.warning(
                        "You have specified `max_concurrent_trials="
                        f"{max_concurrent_trials}`, but the `search_alg` is "
                        "already a `ConcurrencyLimiter`. "
                        "`max_concurrent_trials` will be ignored."
                    )
        else:
            if max_concurrent_trials < 1:
                raise ValueError(
                    "`max_concurrent_trials` must be greater or equal than 1, "
                    f"got {max_concurrent_trials}."
                )
            if isinstance(search_alg, Searcher):
                search_alg = ConcurrencyLimiter(
                    search_alg, max_concurrent=max_concurrent_trials
                )
            elif not is_local_mode:
                logger.warning(
                    "You have passed a `SearchGenerator` instance as the "
                    "`search_alg`, but `max_concurrent_trials` requires a "
                    "`Searcher` instance`. `max_concurrent_trials` "
                    "will be ignored."
                )

    if isinstance(search_alg, Searcher):
        search_alg = SearchGenerator(search_alg)

    if config and not searcher_set_search_props(
        search_alg.set_search_properties,
        metric,
        mode,
        config,
        **experiments[0].public_spec,
    ):
        if _has_unresolved_values(config):
            raise ValueError(
                f"You passed a `{error_message_map['search_space_arg']}` parameter to "
                f"`{error_message_map['entrypoint']}` with "
                "unresolved parameters, but the search algorithm was already "
                "instantiated with a search space. Make sure that `config` "
                "does not contain any more parameter definitions - include "
                "them in the search algorithm's search space if necessary."
            )

    if not scheduler_set_search_props(
        scheduler.set_search_properties, metric, mode, **experiments[0].public_spec
    ):
        raise ValueError(
            "You passed a `metric` or `mode` argument to "
            f"`{error_message_map['entrypoint']}`, but "
            "the scheduler you are using was already instantiated with their "
            "own `metric` and `mode` parameters. Either remove the arguments "
            f"from your scheduler or from `{error_message_map['entrypoint']}` args."
        )

    progress_metrics = _detect_progress_metrics(_get_trainable(run_or_experiment))

    air_usage.tag_storage_type(experiments[0].storage)

    # NOTE: Report callback telemetry before populating the list with default callbacks.
    # This tracks user-specified callback usage.
    air_usage.tag_callbacks(callbacks)

    # Create default logging + syncer callbacks
    callbacks = _create_default_callbacks(
        callbacks,
        air_verbosity=air_verbosity,
        entrypoint=_entrypoint,
        config=config,
        metric=metric,
        mode=mode,
        progress_metrics=progress_metrics,
    )

    # User Warning for GPUs
    if ray.cluster_resources().get("GPU", 0):
        if _check_gpus_in_resources(resources=resources_per_trial):
            # "gpu" is manually set.
            pass
        elif _check_default_resources_override(experiments[0].run_identifier):
            # "default_resources" is manually overridden.
            pass
        else:
            logger.warning(
                "Tune detects GPUs, but no trials are using GPUs. "
                "To enable trials to use GPUs, wrap `train_func` with "
                "`tune.with_resources(train_func, resources_per_trial={'gpu': 1})` "
                "which allows Tune to expose 1 GPU to each trial. "
                "For Ray Train Trainers, you can specify GPU resources "
                "through `ScalingConfig(use_gpu=True)`. "
                "You can also override "
                "`Trainable.default_resource_request` if using the "
                "Trainable API."
            )

    experiment_interrupted_event = _setup_signal_catching()

    if progress_reporter and air_verbosity is not None:
        logger.warning(
            "AIR_VERBOSITY is set, ignoring passed-in ProgressReporter for now."
        )
        progress_reporter = None

    if air_verbosity is None:
        progress_reporter = progress_reporter or _detect_reporter()

    runner_kwargs = dict(
        search_alg=search_alg,
        placeholder_resolvers=placeholder_resolvers,
        scheduler=scheduler,
        stopper=experiments[0].stopper,
        resume=resume,
        server_port=server_port,
        fail_fast=fail_fast,
        callbacks=callbacks,
        metric=metric,
        trial_checkpoint_config=experiments[0].checkpoint_config,
        reuse_actors=reuse_actors,
        storage=experiments[0].storage,
        _trainer_api=_entrypoint == AirEntrypoint.TRAINER,
    )

    runner = TuneController(**runner_kwargs)

    if not runner.resumed:
        for exp in experiments:
            search_alg.add_configurations([exp])
        # search_alg.total_samples has been updated, so we should
        # update the number of pending trials
        runner.update_max_pending_trials()
    else:
        logger.debug(
            "You have resumed the Tune run, which means that any newly specified "
            "`Experiment`s will be ignored. "
            "Tune will just continue what was previously running."
        )
        if resources_per_trial:
            runner.update_pending_trial_resources(resources_per_trial)

    # Calls setup on callbacks
    runner.setup_experiments(
        experiments=experiments, total_num_samples=search_alg.total_samples
    )

    tune_start = time.time()

    air_progress_reporter = None
    if air_verbosity is None:
        progress_reporter.setup(
            start_time=tune_start,
            total_samples=search_alg.total_samples,
            metric=metric,
            mode=mode,
        )
    else:
        from ray.tune.experimental.output import ProgressReporter as AirProgressReporter

        for callback in callbacks:
            if isinstance(callback, AirProgressReporter):
                air_progress_reporter = callback
                air_progress_reporter.setup(
                    start_time=tune_start, total_samples=search_alg.total_samples
                )
                break

    experiment_local_path = runner._storage.experiment_local_path
    experiment_dir_name = runner._storage.experiment_dir_name

    if any(isinstance(cb, TBXLoggerCallback) for cb in callbacks):
        tensorboard_path = experiment_local_path
    else:
        tensorboard_path = None

    if air_progress_reporter:
        air_progress_reporter.experiment_started(
            experiment_name=experiment_dir_name,
            experiment_path=runner.experiment_path,
            searcher_str=search_alg.__class__.__name__,
            scheduler_str=scheduler.__class__.__name__,
            total_num_samples=search_alg.total_samples,
            tensorboard_path=tensorboard_path,
        )

    try:
        while not runner.is_finished() and not experiment_interrupted_event.is_set():
            runner.step()
            if has_verbosity(Verbosity.V1_EXPERIMENT):
                _report_progress(runner, progress_reporter)

            if air_verbosity is not None:
                _report_air_progress(runner, air_progress_reporter)
    except Exception:
        runner.cleanup()
        raise

    tune_taken = time.time() - tune_start

    try:
        runner.checkpoint(force=True, wait=True)
    except Exception as e:
        logger.warning(f"Trial Runner checkpointing failed: {str(e)}")

    if has_verbosity(Verbosity.V1_EXPERIMENT):
        _report_progress(runner, progress_reporter, done=True)

    if air_verbosity is not None:
        _report_air_progress(runner, air_progress_reporter, force=True)

    all_trials = runner.get_trials()

    runner.cleanup()

    incomplete_trials = []
    for trial in all_trials:
        if trial.status != Trial.TERMINATED:
            incomplete_trials += [trial]

    if incomplete_trials:
        if raise_on_failed_trial and not experiment_interrupted_event.is_set():
            raise TuneError("Trials did not complete", incomplete_trials)
        else:
            logger.error("Trials did not complete: %s", incomplete_trials)

    all_taken = time.time() - all_start
    if has_verbosity(Verbosity.V1_EXPERIMENT):
        logger.info(
            f"Total run time: {all_taken:.2f} seconds "
            f"({tune_taken:.2f} seconds for the tuning loop)."
        )

    if experiment_interrupted_event.is_set():
        restore_entrypoint = error_message_map["restore_entrypoint"].format(
            path=runner.experiment_path,
        )
        if _entrypoint == AirEntrypoint.TRAINER:
            logger.warning(
                f"Training has been interrupted, but the most recent state was saved.\n"
                f"Resume training with: {restore_entrypoint}"
            )
        else:
            logger.warning(
                f"Experiment has been interrupted, but the most recent state was "
                f"saved.\nResume experiment with: {restore_entrypoint}"
            )

    return ExperimentAnalysis(
        experiment_checkpoint_path=runner.experiment_path,
        default_metric=metric,
        default_mode=mode,
        trials=all_trials,
        storage_filesystem=experiments[0].storage.storage_filesystem,
    )


@PublicAPI
def run_experiments(
    experiments: Union[Experiment, Mapping, Sequence[Union[Experiment, Mapping]]],
    scheduler: Optional[TrialScheduler] = None,
    server_port: Optional[int] = None,
    verbose: Optional[Union[int, AirVerbosity, Verbosity]] = None,
    progress_reporter: Optional[ProgressReporter] = None,
    resume: Union[bool, str] = False,
    reuse_actors: Optional[bool] = None,
    raise_on_failed_trial: bool = True,
    concurrent: bool = True,
    callbacks: Optional[Sequence[Callback]] = None,
    _remote: Optional[bool] = None,
):
    """Runs and blocks until all trials finish.

    Example:
        >>> from ray.tune.experiment import Experiment
        >>> from ray.tune.tune import run_experiments
        >>> def my_func(config): return {"score": 0}
        >>> experiment_spec = Experiment("experiment", my_func) # doctest: +SKIP
        >>> run_experiments(experiments=experiment_spec) # doctest: +SKIP
        >>> experiment_spec = {"experiment": {"run": my_func}} # doctest: +SKIP
        >>> run_experiments(experiments=experiment_spec) # doctest: +SKIP

    Returns:
        List of Trial objects, holding data for each executed trial.

    """
    if _remote is None:
        _remote = ray.util.client.ray.is_connected()

    _ray_auto_init(entrypoint="tune.run_experiments(...)")

    if verbose is None:
        # Default `verbose` value. For new output engine, this is AirVerbosity.VERBOSE.
        # For old output engine, this is Verbosity.V3_TRIAL_DETAILS
        verbose = get_air_verbosity(AirVerbosity.VERBOSE) or Verbosity.V3_TRIAL_DETAILS

    if _remote:
        if get_air_verbosity(verbose) is not None:
            logger.info(
                "[output] This uses the legacy output and progress reporter, "
                "as Ray client is not supported by the new engine. "
                "For more information, see "
                "https://github.com/ray-project/ray/issues/36949"
            )
        remote_run = ray.remote(num_cpus=0)(run_experiments)

        # Make sure tune.run_experiments is run on the server node.
        remote_run = _force_on_current_node(remote_run)

        return ray.get(
            remote_run.remote(
                experiments,
                scheduler,
                server_port,
                verbose,
                progress_reporter,
                resume,
                reuse_actors,
                raise_on_failed_trial,
                concurrent,
                callbacks,
                _remote=False,
            )
        )

    # This is important to do this here
    # because it schematize the experiments
    # and it conducts the implicit registration.
    experiments = _convert_to_experiment_list(experiments)

    if concurrent:
        return run(
            experiments,
            server_port=server_port,
            verbose=verbose,
            progress_reporter=progress_reporter,
            resume=resume,
            reuse_actors=reuse_actors,
            raise_on_failed_trial=raise_on_failed_trial,
            scheduler=scheduler,
            callbacks=callbacks,
            _entrypoint=AirEntrypoint.TUNE_RUN_EXPERIMENTS,
        ).trials
    else:
        trials = []
        for exp in experiments:
            trials += run(
                exp,
                server_port=server_port,
                verbose=verbose,
                progress_reporter=progress_reporter,
                resume=resume,
                reuse_actors=reuse_actors,
                raise_on_failed_trial=raise_on_failed_trial,
                scheduler=scheduler,
                callbacks=callbacks,
                _entrypoint=AirEntrypoint.TUNE_RUN_EXPERIMENTS,
            ).trials
        return trials

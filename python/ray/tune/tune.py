from typing import Any, Callable, Dict, Mapping, Optional, Sequence, Type, Union

import datetime
import logging
import os
import signal
import sys
import time
import warnings

import ray
from ray.util.annotations import PublicAPI
from ray.util.ml_utils.node import force_on_current_node
from ray.util.queue import Queue, Empty

from ray.tune.analysis import ExperimentAnalysis
from ray.tune.callback import Callback
from ray.tune.error import TuneError
from ray.tune.experiment import Experiment, convert_to_experiment_list
from ray.tune.logger import Logger
from ray.tune.progress_reporter import (
    detect_reporter,
    ProgressReporter,
    JupyterNotebookReporter,
)
from ray.tune.ray_trial_executor import RayTrialExecutor
from ray.tune.registry import get_trainable_cls
from ray.tune.schedulers import PopulationBasedTraining, PopulationBasedTrainingReplay
from ray.tune.stopper import Stopper
from ray.tune.suggest import BasicVariantGenerator, SearchAlgorithm, SearchGenerator
from ray.tune.suggest.suggestion import ConcurrencyLimiter, Searcher
from ray.tune.suggest.util import set_search_properties_backwards_compatible
from ray.tune.suggest.variant_generator import has_unresolved_values
from ray.tune.syncer import SyncConfig, set_sync_periods, wait_for_sync
from ray.tune.trainable import Trainable
from ray.tune.trial import Trial
from ray.tune.trial_runner import TrialRunner
from ray.tune.utils.callback import create_default_callbacks
from ray.tune.utils.log import Verbosity, has_verbosity, set_verbosity

# Must come last to avoid circular imports
from ray.tune.schedulers import FIFOScheduler, TrialScheduler
from ray.tune.utils.placement_groups import PlacementGroupFactory

logger = logging.getLogger(__name__)


def _check_default_resources_override(run_identifier):
    if not isinstance(run_identifier, str):
        # If obscure dtype, assume it is overridden.
        return True
    trainable_cls = get_trainable_cls(run_identifier)
    return hasattr(trainable_cls, "default_resource_request") and (
        trainable_cls.default_resource_request.__code__
        != Trainable.default_resource_request.__code__
    )


def _report_progress(runner, reporter, done=False):
    """Reports experiment progress.

    Args:
        runner (TrialRunner): Trial runner to report on.
        reporter (ProgressReporter): Progress reporter.
        done (bool): Whether this is the last progress report attempt.
    """
    trials = runner.get_trials()
    if reporter.should_report(trials, done=done):
        sched_debug_str = runner.scheduler_alg.debug_string()
        executor_debug_str = runner.trial_executor.debug_string()
        reporter.report(trials, done, sched_debug_str, executor_debug_str)


@PublicAPI
def run(
    run_or_experiment: Union[str, Callable, Type],
    name: Optional[str] = None,
    metric: Optional[str] = None,
    mode: Optional[str] = None,
    stop: Union[None, Mapping, Stopper, Callable[[str, Mapping], bool]] = None,
    time_budget_s: Union[None, int, float, datetime.timedelta] = None,
    config: Optional[Dict[str, Any]] = None,
    resources_per_trial: Union[
        None, Mapping[str, Union[float, int, Mapping]], PlacementGroupFactory
    ] = None,
    num_samples: int = 1,
    local_dir: Optional[str] = None,
    search_alg: Optional[Union[Searcher, SearchAlgorithm, str]] = None,
    scheduler: Optional[Union[TrialScheduler, str]] = None,
    keep_checkpoints_num: Optional[int] = None,
    checkpoint_score_attr: Optional[str] = None,
    checkpoint_freq: int = 0,
    checkpoint_at_end: bool = False,
    verbose: Union[int, Verbosity] = Verbosity.V3_TRIAL_DETAILS,
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
    resume: bool = False,
    reuse_actors: bool = False,
    trial_executor: Optional[RayTrialExecutor] = None,
    raise_on_failed_trial: bool = True,
    callbacks: Optional[Sequence[Callback]] = None,
    max_concurrent_trials: Optional[int] = None,
    # Deprecated args
    queue_trials: Optional[bool] = None,
    loggers: Optional[Sequence[Type[Logger]]] = None,
    _remote: Optional[bool] = None,
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
        run_or_experiment (function | class | str | :class:`Experiment`): If
            function|class|str, this is the algorithm or model to train.
            This may refer to the name of a built-on algorithm
            (e.g. RLLib's DQN or PPO), a user-defined trainable
            function or class, or the string identifier of a
            trainable function or class registered in the tune registry.
            If Experiment, then Tune will execute training based on
            Experiment.spec. If you want to pass in a Python lambda, you
            will need to first register the function:
            ``tune.register_trainable("lambda_id", lambda x: ...)``. You can
            then use ``tune.run("lambda_id")``.
        metric (str): Metric to optimize. This metric should be reported
            with `tune.report()`. If set, will be passed to the search
            algorithm and scheduler.
        mode (str): Must be one of [min, max]. Determines whether objective is
            minimizing or maximizing the metric attribute. If set, will be
            passed to the search algorithm and scheduler.
        name (str): Name of experiment.
        stop (dict | callable | :class:`Stopper`): Stopping criteria. If dict,
            the keys may be any field in the return result of 'train()',
            whichever is reached first. If function, it must take (trial_id,
            result) as arguments and return a boolean (True if trial should be
            stopped, False otherwise). This can also be a subclass of
            ``ray.tune.Stopper``, which allows users to implement
            custom experiment-wide stopping (i.e., stopping an entire Tune
            run based on some time constraint).
        time_budget_s (int|float|datetime.timedelta): Global time budget in
            seconds after which all trials are stopped. Can also be a
            ``datetime.timedelta`` object.
        config (dict): Algorithm-specific configuration for Tune variant
            generation (e.g. env, hyperparams). Defaults to empty dict.
            Custom search algorithms may ignore this.
        resources_per_trial (dict|PlacementGroupFactory): Machine resources
            to allocate per trial, e.g. ``{"cpu": 64, "gpu": 8}``.
            Note that GPUs will not be assigned unless you specify them here.
            Defaults to 1 CPU and 0 GPUs in
            ``Trainable.default_resource_request()``. This can also
            be a PlacementGroupFactory object wrapping arguments to create a
            per-trial placement group.
        num_samples (int): Number of times to sample from the
            hyperparameter space. Defaults to 1. If `grid_search` is
            provided as an argument, the grid will be repeated
            `num_samples` of times. If this is -1, (virtually) infinite
            samples are generated until a stopping condition is met.
        local_dir (str): Local dir to save training results to.
            Defaults to ``~/ray_results``.
        search_alg (Searcher|SearchAlgorithm|str): Search algorithm for
            optimization. You can also use the name of the algorithm.
        scheduler (TrialScheduler|str): Scheduler for executing
            the experiment. Choose among FIFO (default), MedianStopping,
            AsyncHyperBand, HyperBand and PopulationBasedTraining. Refer to
            ray.tune.schedulers for more options. You can also use the
            name of the scheduler.
        keep_checkpoints_num (int): Number of checkpoints to keep. A value of
            `None` keeps all checkpoints. Defaults to `None`. If set, need
            to provide `checkpoint_score_attr`.
        checkpoint_score_attr (str): Specifies by which attribute to rank the
            best checkpoint. Default is increasing order. If attribute starts
            with `min-` it will rank attribute in decreasing order, i.e.
            `min-validation_loss`.
        checkpoint_freq (int): How many training iterations between
            checkpoints. A value of 0 (default) disables checkpointing.
            This has no effect when using the Functional Training API.
        checkpoint_at_end (bool): Whether to checkpoint at the end of the
            experiment regardless of the checkpoint_freq. Default is False.
            This has no effect when using the Functional Training API.
        verbose (Union[int, Verbosity]): 0, 1, 2, or 3. Verbosity mode.
            0 = silent, 1 = only status updates, 2 = status and brief trial
            results, 3 = status and detailed trial results. Defaults to 3.
        progress_reporter (ProgressReporter): Progress reporter for reporting
            intermediate experiment progress. Defaults to CLIReporter if
            running in command-line, or JupyterNotebookReporter if running in
            a Jupyter notebook.
        log_to_file (bool|str|Sequence): Log stdout and stderr to files in
            Tune's trial directories. If this is `False` (default), no files
            are written. If `true`, outputs are written to `trialdir/stdout`
            and `trialdir/stderr`, respectively. If this is a single string,
            this is interpreted as a file relative to the trialdir, to which
            both streams are written. If this is a Sequence (e.g. a Tuple),
            it has to have length 2 and the elements indicate the files to
            which stdout and stderr are written, respectively.
        trial_name_creator (Callable[[Trial], str]): Optional function
            for generating the trial string representation.
        trial_dirname_creator (Callable[[Trial], str]): Function
            for generating the trial dirname. This function should take
            in a Trial object and return a string representing the
            name of the directory. The return value cannot be a path.
        sync_config (SyncConfig): Configuration object for syncing. See
            tune.SyncConfig.
        export_formats (list): List of formats that exported at the end of
            the experiment. Default is None.
        max_failures (int): Try to recover a trial at least this many times.
            Ray will recover from the latest checkpoint if present.
            Setting to -1 will lead to infinite recovery retries.
            Setting to 0 will disable retries. Defaults to 0.
        fail_fast (bool | str): Whether to fail upon the first error.
            If fail_fast='raise' provided, Tune will automatically
            raise the exception received by the Trainable. fail_fast='raise'
            can easily leak resources and should be used with caution (it
            is best used with `ray.init(local_mode=True)`).
        restore (str): Path to checkpoint. Only makes sense to set if
            running 1 trial. Defaults to None.
        server_port (int): Port number for launching TuneServer.
        resume (str|bool): One of "LOCAL", "REMOTE", "PROMPT", "ERRORED_ONLY",
            or bool. LOCAL/True restores the checkpoint from the
            local experiment directory, determined
            by ``name`` and ``local_dir``. REMOTE restores the checkpoint
            from ``upload_dir`` (as passed to ``sync_config``).
            PROMPT provides CLI feedback.
            False forces a new experiment. ERRORED_ONLY resets and reruns
            ERRORED trials upon resume - previous trial artifacts will
            be left untouched.  If resume is set but checkpoint does not exist,
            ValueError will be thrown.
        reuse_actors (bool): Whether to reuse actors between different trials
            when possible. This can drastically speed up experiments that start
            and stop actors often (e.g., PBT in time-multiplexing mode). This
            requires trials to have the same resource requirements.
        trial_executor (TrialExecutor): Manage the execution of trials.
        raise_on_failed_trial (bool): Raise TuneError if there exists failed
            trial (of ERROR state) when the experiments complete.
        callbacks (list): List of callbacks that will be called at different
            times in the training loop. Must be instances of the
            ``ray.tune.callback.Callback`` class. If not passed,
            `LoggerCallback` and `SyncerCallback` callbacks are automatically
            added.
        max_concurrent_trials (int): Maximum number of trials to run
            concurrently. Must be non-negative. If None or 0, no limit will
            be applied. This is achieved by wrapping the ``search_alg`` in
            a :class:`ConcurrencyLimiter`, and thus setting this argument
            will raise an exception if the ``search_alg`` is already a
            :class:`ConcurrencyLimiter`. Defaults to None.
        _remote (bool): Whether to run the Tune driver in a remote function.
            This is disabled automatically if a custom trial executor is
            passed in. This is enabled by default in Ray client mode.

    Returns:
        ExperimentAnalysis: Object for experiment analysis.

    Raises:
        TuneError: Any trials failed and `raise_on_failed_trial` is True.
    """

    # To be removed in 1.9.
    if queue_trials is not None:
        raise DeprecationWarning(
            "`queue_trials` has been deprecated and is replaced by "
            "the `TUNE_MAX_PENDING_TRIALS_PG` environment variable. "
            "Per default at least one Trial is queued at all times, "
            "so you likely don't need to change anything other than "
            "removing this argument from your call to `tune.run()`"
        )

    # NO CODE IS TO BE ADDED ABOVE THIS COMMENT
    # remote_run_kwargs must be defined before any other
    # code is ran to ensure that at this point,
    # `locals()` is equal to args and kwargs
    remote_run_kwargs = locals().copy()
    remote_run_kwargs.pop("_remote")

    if _remote is None:
        _remote = ray.util.client.ray.is_connected()

    if _remote is True and trial_executor:
        raise ValueError("cannot use custom trial executor")

    if not trial_executor or isinstance(trial_executor, RayTrialExecutor):
        _ray_auto_init()

    if _remote:
        remote_run = ray.remote(num_cpus=0)(run)

        # Make sure tune.run is called on the sever node.
        remote_run = force_on_current_node(remote_run)

        # JupyterNotebooks don't work with remote tune runs out of the box
        # (e.g. via Ray client) as they don't have access to the main
        # process stdout. So we introduce a queue here that accepts
        # callables, which will then be executed on the driver side.
        if isinstance(progress_reporter, JupyterNotebookReporter):
            execute_queue = Queue(
                actor_options={"num_cpus": 0, **force_on_current_node(None)}
            )
            progress_reporter.set_output_queue(execute_queue)

            def get_next_queue_item():
                try:
                    return execute_queue.get(block=False)
                except Empty:
                    return None

        else:
            # If we don't need a queue, use this dummy get fn instead of
            # scheduling an unneeded actor
            def get_next_queue_item():
                return None

        def _handle_execute_queue():
            execute_item = get_next_queue_item()
            while execute_item:
                if isinstance(execute_item, Callable):
                    execute_item()

                execute_item = get_next_queue_item()

        remote_future = remote_run.remote(_remote=False, **remote_run_kwargs)

        # ray.wait(...)[1] returns futures that are not ready, yet
        while ray.wait([remote_future], timeout=0.2)[1]:
            # Check if we have items to execute
            _handle_execute_queue()

        # Handle queue one last time
        _handle_execute_queue()

        return ray.get(remote_future)

    del remote_run_kwargs

    all_start = time.time()

    if loggers:
        # Raise DeprecationWarning in 1.9, remove in 1.10/1.11
        warnings.warn(
            "The `loggers` argument is deprecated. Please pass the respective "
            "`LoggerCallback` classes to the `callbacks` argument instead. "
            "See https://docs.ray.io/en/latest/tune/api_docs/logging.html"
        )

    if mode and mode not in ["min", "max"]:
        raise ValueError(
            "The `mode` parameter passed to `tune.run()` has to be one of "
            "['min', 'max']"
        )

    set_verbosity(verbose)

    config = config or {}
    sync_config = sync_config or SyncConfig()
    set_sync_periods(sync_config)

    if num_samples == -1:
        num_samples = sys.maxsize

    result_buffer_length = None

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
        result_buffer_length = 1

    if (
        isinstance(scheduler, (PopulationBasedTraining, PopulationBasedTrainingReplay))
        and not reuse_actors
    ):
        warnings.warn(
            "Consider boosting PBT performance by enabling `reuse_actors` as "
            "well as implementing `reset_config` for Trainable."
        )

    trial_executor = trial_executor or RayTrialExecutor(
        reuse_actors=reuse_actors, result_buffer_length=result_buffer_length
    )
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
                local_dir=local_dir,
                sync_config=sync_config,
                trial_name_creator=trial_name_creator,
                trial_dirname_creator=trial_dirname_creator,
                log_to_file=log_to_file,
                checkpoint_freq=checkpoint_freq,
                checkpoint_at_end=checkpoint_at_end,
                keep_checkpoints_num=keep_checkpoints_num,
                checkpoint_score_attr=checkpoint_score_attr,
                export_formats=export_formats,
                max_failures=max_failures,
                restore=restore,
            )
    else:
        logger.debug("Ignoring some parameters passed into tune.run.")

    if fail_fast and max_failures != 0:
        raise ValueError("max_failures must be 0 if fail_fast=True.")

    if isinstance(search_alg, str):
        # importing at top level causes a recursive dependency
        from ray.tune.suggest import create_searcher

        search_alg = create_searcher(search_alg)

    # if local_mode=True is set during ray.init().
    is_local_mode = ray.worker._mode() == ray.worker.LOCAL_MODE

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

    if config and not set_search_properties_backwards_compatible(
        search_alg.set_search_properties,
        metric,
        mode,
        config,
        **experiments[0].public_spec,
    ):
        if has_unresolved_values(config):
            raise ValueError(
                "You passed a `config` parameter to `tune.run()` with "
                "unresolved parameters, but the search algorithm was already "
                "instantiated with a search space. Make sure that `config` "
                "does not contain any more parameter definitions - include "
                "them in the search algorithm's search space if necessary."
            )

    if not scheduler.set_search_properties(metric, mode):
        raise ValueError(
            "You passed a `metric` or `mode` argument to `tune.run()`, but "
            "the scheduler you are using was already instantiated with their "
            "own `metric` and `mode` parameters. Either remove the arguments "
            "from your scheduler or from your call to `tune.run()`"
        )

    # Create syncer callbacks
    callbacks = create_default_callbacks(
        callbacks, sync_config, metric=metric, loggers=loggers
    )

    runner = TrialRunner(
        search_alg=search_alg,
        scheduler=scheduler,
        local_checkpoint_dir=experiments[0].checkpoint_dir,
        remote_checkpoint_dir=experiments[0].remote_checkpoint_dir,
        sync_config=sync_config,
        stopper=experiments[0].stopper,
        resume=resume,
        server_port=server_port,
        fail_fast=fail_fast,
        trial_executor=trial_executor,
        callbacks=callbacks,
        metric=metric,
        # Driver should only sync trial checkpoints if
        # checkpoints are not synced to cloud
        driver_sync_trial_checkpoints=not bool(sync_config.upload_dir),
    )

    if not runner.resumed:
        for exp in experiments:
            search_alg.add_configurations([exp])
    else:
        logger.info(
            "TrialRunner resumed, ignoring new add_experiment but "
            "updating trial resources."
        )
        if resources_per_trial:
            runner.update_pending_trial_resources(resources_per_trial)

    progress_reporter = progress_reporter or detect_reporter()

    if not progress_reporter.set_search_properties(metric, mode):
        raise ValueError(
            "You passed a `metric` or `mode` argument to `tune.run()`, but "
            "the reporter you are using was already instantiated with their "
            "own `metric` and `mode` parameters. Either remove the arguments "
            "from your reporter or from your call to `tune.run()`"
        )
    progress_reporter.set_total_samples(search_alg.total_samples)

    # Calls setup on callbacks
    runner.setup_experiments(
        experiments=experiments, total_num_samples=search_alg.total_samples
    )

    # User Warning for GPUs
    if trial_executor.has_gpus():
        if isinstance(resources_per_trial, dict) and "gpu" in resources_per_trial:
            # "gpu" is manually set.
            pass
        elif _check_default_resources_override(experiments[0].run_identifier):
            # "default_resources" is manually overridden.
            pass
        else:
            logger.warning(
                "Tune detects GPUs, but no trials are using GPUs. "
                "To enable trials to use GPUs, set "
                "tune.run(resources_per_trial={'gpu': 1}...) "
                "which allows Tune to expose 1 GPU to each trial. "
                "You can also override "
                "`Trainable.default_resource_request` if using the "
                "Trainable API."
            )

    original_handler = signal.getsignal(signal.SIGINT)
    state = {signal.SIGINT: False}

    def sigint_handler(sig, frame):
        logger.warning(
            "SIGINT received (e.g. via Ctrl+C), ending Ray Tune run. "
            "This will try to checkpoint the experiment state one last time. "
            "Press CTRL+C one more time (or send SIGINT/SIGKILL/SIGTERM) "
            "to skip. "
        )
        state[signal.SIGINT] = True
        # Restore original signal handler to react to future SIGINT signals
        signal.signal(signal.SIGINT, original_handler)

    if not int(os.getenv("TUNE_DISABLE_SIGINT_HANDLER", "0")):
        signal.signal(signal.SIGINT, sigint_handler)

    tune_start = time.time()
    progress_reporter.set_start_time(tune_start)
    while not runner.is_finished() and not state[signal.SIGINT]:
        runner.step()
        if has_verbosity(Verbosity.V1_EXPERIMENT):
            _report_progress(runner, progress_reporter)
    tune_taken = time.time() - tune_start

    try:
        runner.checkpoint(force=True)
    except Exception as e:
        logger.warning(f"Trial Runner checkpointing failed: {str(e)}")

    if has_verbosity(Verbosity.V1_EXPERIMENT):
        _report_progress(runner, progress_reporter, done=True)

    wait_for_sync()
    runner.cleanup()

    incomplete_trials = []
    for trial in runner.get_trials():
        if trial.status != Trial.TERMINATED:
            incomplete_trials += [trial]

    if incomplete_trials:
        if raise_on_failed_trial and not state[signal.SIGINT]:
            raise TuneError("Trials did not complete", incomplete_trials)
        else:
            logger.error("Trials did not complete: %s", incomplete_trials)

    all_taken = time.time() - all_start
    if has_verbosity(Verbosity.V1_EXPERIMENT):
        logger.info(
            f"Total run time: {all_taken:.2f} seconds "
            f"({tune_taken:.2f} seconds for the tuning loop)."
        )

    if state[signal.SIGINT]:
        logger.warning(
            "Experiment has been interrupted, but the most recent state was "
            "saved. You can continue running this experiment by passing "
            "`resume=True` to `tune.run()`"
        )

    trials = runner.get_trials()
    return ExperimentAnalysis(
        runner.checkpoint_file,
        trials=trials,
        default_metric=metric,
        default_mode=mode,
        sync_config=sync_config,
    )


@PublicAPI
def run_experiments(
    experiments: Union[Experiment, Mapping, Sequence[Union[Experiment, Mapping]]],
    scheduler: Optional[TrialScheduler] = None,
    server_port: Optional[int] = None,
    verbose: Union[int, Verbosity] = Verbosity.V3_TRIAL_DETAILS,
    progress_reporter: Optional[ProgressReporter] = None,
    resume: bool = False,
    reuse_actors: bool = False,
    trial_executor: Optional[RayTrialExecutor] = None,
    raise_on_failed_trial: bool = True,
    concurrent: bool = True,
    # Deprecated args.
    queue_trials: Optional[bool] = None,
    callbacks: Optional[Sequence[Callback]] = None,
    _remote: Optional[bool] = None,
):
    """Runs and blocks until all trials finish.

    Examples:
        >>> experiment_spec = Experiment("experiment", my_func)
        >>> run_experiments(experiments=experiment_spec)

        >>> experiment_spec = {"experiment": {"run": my_func}}
        >>> run_experiments(experiments=experiment_spec)

    Returns:
        List of Trial objects, holding data for each executed trial.

    """
    # To be removed in 1.9.
    if queue_trials is not None:
        raise DeprecationWarning(
            "`queue_trials` has been deprecated and is replaced by "
            "the `TUNE_MAX_PENDING_TRIALS_PG` environment variable. "
            "Per default at least one Trial is queued at all times, "
            "so you likely don't need to change anything other than "
            "removing this argument from your call to `tune.run()`"
        )

    if _remote is None:
        _remote = ray.util.client.ray.is_connected()

    if _remote is True and trial_executor:
        raise ValueError("cannot use custom trial executor")

    if not trial_executor or isinstance(trial_executor, RayTrialExecutor):
        _ray_auto_init()

    if _remote:
        remote_run = ray.remote(num_cpus=0)(run_experiments)

        # Make sure tune.run_experiments is run on the server node.
        remote_run = force_on_current_node(remote_run)

        return ray.get(
            remote_run.remote(
                experiments,
                scheduler,
                server_port,
                verbose,
                progress_reporter,
                resume,
                reuse_actors,
                trial_executor,
                raise_on_failed_trial,
                concurrent,
                callbacks,
                _remote=False,
            )
        )

    # This is important to do this here
    # because it schematize the experiments
    # and it conducts the implicit registration.
    experiments = convert_to_experiment_list(experiments)

    if concurrent:
        return run(
            experiments,
            server_port=server_port,
            verbose=verbose,
            progress_reporter=progress_reporter,
            resume=resume,
            reuse_actors=reuse_actors,
            trial_executor=trial_executor,
            raise_on_failed_trial=raise_on_failed_trial,
            scheduler=scheduler,
            callbacks=callbacks,
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
                trial_executor=trial_executor,
                raise_on_failed_trial=raise_on_failed_trial,
                scheduler=scheduler,
                callbacks=callbacks,
            ).trials
        return trials


def _ray_auto_init():
    """Initialize Ray unless already configured."""
    if os.environ.get("TUNE_DISABLE_AUTO_INIT") == "1":
        logger.info("'TUNE_DISABLE_AUTO_INIT=1' detected.")
    elif not ray.is_initialized():
        logger.info(
            "Initializing Ray automatically."
            "For cluster usage or custom Ray initialization, "
            "call `ray.init(...)` before `tune.run`."
        )
        ray.init()

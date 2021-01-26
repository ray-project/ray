import logging
import sys
import time

from ray.tune.error import TuneError
from ray.tune.experiment import convert_to_experiment_list, Experiment
from ray.tune.analysis import ExperimentAnalysis
from ray.tune.suggest import BasicVariantGenerator, SearchGenerator
from ray.tune.suggest.suggestion import Searcher
from ray.tune.suggest.variant_generator import has_unresolved_values
from ray.tune.trial import Trial
from ray.tune.trainable import Trainable
from ray.tune.ray_trial_executor import RayTrialExecutor
from ray.tune.utils.callback import create_default_callbacks
from ray.tune.registry import get_trainable_cls
from ray.tune.syncer import wait_for_sync, set_sync_periods, \
    SyncConfig
from ray.tune.trial_runner import TrialRunner
from ray.tune.progress_reporter import CLIReporter, JupyterNotebookReporter
from ray.tune.schedulers import FIFOScheduler
from ray.tune.utils.log import Verbosity, has_verbosity, set_verbosity

logger = logging.getLogger(__name__)

try:
    class_name = get_ipython().__class__.__name__
    IS_NOTEBOOK = True if "Terminal" not in class_name else False
except NameError:
    IS_NOTEBOOK = False


def _check_default_resources_override(run_identifier):
    if not isinstance(run_identifier, str):
        # If obscure dtype, assume it is overridden.
        return True
    trainable_cls = get_trainable_cls(run_identifier)
    return hasattr(trainable_cls, "default_resource_request") and (
        trainable_cls.default_resource_request.__code__ !=
        Trainable.default_resource_request.__code__)


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


def run(
        run_or_experiment,
        name=None,
        metric=None,
        mode=None,
        stop=None,
        time_budget_s=None,
        config=None,
        resources_per_trial=None,
        num_samples=1,
        local_dir=None,
        search_alg=None,
        scheduler=None,
        keep_checkpoints_num=None,
        checkpoint_score_attr=None,
        checkpoint_freq=0,
        checkpoint_at_end=False,
        verbose=Verbosity.V3_TRIAL_DETAILS,
        progress_reporter=None,
        log_to_file=False,
        trial_name_creator=None,
        trial_dirname_creator=None,
        sync_config=None,
        export_formats=None,
        max_failures=0,
        fail_fast=False,
        restore=None,
        server_port=None,
        resume=False,
        queue_trials=False,
        reuse_actors=False,
        trial_executor=None,
        raise_on_failed_trial=True,
        callbacks=None,
        # Deprecated args
        loggers=None,
        ray_auto_init=None,
        run_errored_only=None,
        global_checkpoint_period=None,
        with_server=None,
        upload_dir=None,
        sync_to_cloud=None,
        sync_to_driver=None,
        sync_on_checkpoint=None,
):
    """Executes training.

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
        resources_per_trial (dict|Callable): Machine resources to allocate per
            trial, e.g. ``{"cpu": 64, "gpu": 8}``. Note that GPUs will not be
            assigned unless you specify them here. Defaults to 1 CPU and 0
            GPUs in ``Trainable.default_resource_request()``. This can also
            be a function returning a placement group.
        num_samples (int): Number of times to sample from the
            hyperparameter space. Defaults to 1. If `grid_search` is
            provided as an argument, the grid will be repeated
            `num_samples` of times. If this is -1, (virtually) infinite
            samples are generated until a stopping condition is met.
        local_dir (str): Local dir to save training results to.
            Defaults to ``~/ray_results``.
        search_alg (Searcher|SearchAlgorithm): Search algorithm for
            optimization.
        scheduler (TrialScheduler): Scheduler for executing
            the experiment. Choose among FIFO (default), MedianStopping,
            AsyncHyperBand, HyperBand and PopulationBasedTraining. Refer to
            ray.tune.schedulers for more options.
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
            local_checkpoint_dir, determined
            by `name` and `local_dir`. REMOTE restores the checkpoint
            from remote_checkpoint_dir. PROMPT provides CLI feedback.
            False forces a new experiment. ERRORED_ONLY resets and reruns
            ERRORED trials upon resume - previous trial artifacts will
            be left untouched.  If resume is set but checkpoint does not exist,
            ValueError will be thrown.
        queue_trials (bool): Whether to queue trials when the cluster does
            not currently have enough resources to launch one. This should
            be set to True when running on an autoscaling cluster to enable
            automatic scale-up.
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


    Returns:
        ExperimentAnalysis: Object for experiment analysis.

    Raises:
        TuneError: Any trials failed and `raise_on_failed_trial` is True.
    """
    all_start = time.time()
    if global_checkpoint_period:
        raise ValueError("global_checkpoint_period is deprecated. Set env var "
                         "'TUNE_GLOBAL_CHECKPOINT_S' instead.")
    if ray_auto_init:
        raise ValueError("ray_auto_init is deprecated. "
                         "Set env var 'TUNE_DISABLE_AUTO_INIT=1' instead or "
                         "call 'ray.init' before calling 'tune.run'.")
    if with_server:
        raise ValueError(
            "with_server is deprecated. It is now enabled by default "
            "if 'server_port' is not None.")
    if sync_on_checkpoint or sync_to_cloud or sync_to_driver or upload_dir:
        raise ValueError(
            "sync_on_checkpoint / sync_to_cloud / sync_to_driver / "
            "upload_dir must now be set via `tune.run("
            "sync_config=SyncConfig(...)`. See `ray.tune.SyncConfig` for "
            "more details.")

    if mode and mode not in ["min", "max"]:
        raise ValueError(
            "The `mode` parameter passed to `tune.run()` has to be one of "
            "['min', 'max']")

    set_verbosity(verbose)

    config = config or {}
    sync_config = sync_config or SyncConfig()
    set_sync_periods(sync_config)

    if num_samples == -1:
        num_samples = sys.maxsize

    trial_executor = trial_executor or RayTrialExecutor(
        reuse_actors=reuse_actors, queue_trials=queue_trials)
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
                upload_dir=sync_config.upload_dir,
                sync_to_driver=sync_config.sync_to_driver,
                trial_name_creator=trial_name_creator,
                trial_dirname_creator=trial_dirname_creator,
                log_to_file=log_to_file,
                checkpoint_freq=checkpoint_freq,
                checkpoint_at_end=checkpoint_at_end,
                sync_on_checkpoint=sync_config.sync_on_checkpoint,
                keep_checkpoints_num=keep_checkpoints_num,
                checkpoint_score_attr=checkpoint_score_attr,
                export_formats=export_formats,
                max_failures=max_failures,
                restore=restore)
    else:
        logger.debug("Ignoring some parameters passed into tune.run.")

    if sync_config.sync_to_cloud:
        for exp in experiments:
            assert exp.remote_checkpoint_dir, (
                "Need `upload_dir` if `sync_to_cloud` given.")

    if fail_fast and max_failures != 0:
        raise ValueError("max_failures must be 0 if fail_fast=True.")

    if issubclass(type(search_alg), Searcher):
        search_alg = SearchGenerator(search_alg)

    if not search_alg:
        search_alg = BasicVariantGenerator()

    if config and not search_alg.set_search_properties(metric, mode, config):
        if has_unresolved_values(config):
            raise ValueError(
                "You passed a `config` parameter to `tune.run()` with "
                "unresolved parameters, but the search algorithm was already "
                "instantiated with a search space. Make sure that `config` "
                "does not contain any more parameter definitions - include "
                "them in the search algorithm's search space if necessary.")

    scheduler = scheduler or FIFOScheduler()
    if not scheduler.set_search_properties(metric, mode):
        raise ValueError(
            "You passed a `metric` or `mode` argument to `tune.run()`, but "
            "the scheduler you are using was already instantiated with their "
            "own `metric` and `mode` parameters. Either remove the arguments "
            "from your scheduler or from your call to `tune.run()`")

    # Create syncer callbacks
    callbacks = create_default_callbacks(
        callbacks, sync_config, metric=metric, loggers=loggers)

    runner = TrialRunner(
        search_alg=search_alg,
        scheduler=scheduler,
        local_checkpoint_dir=experiments[0].checkpoint_dir,
        remote_checkpoint_dir=experiments[0].remote_checkpoint_dir,
        sync_to_cloud=sync_config.sync_to_cloud,
        stopper=experiments[0].stopper,
        resume=resume,
        server_port=server_port,
        fail_fast=fail_fast,
        trial_executor=trial_executor,
        callbacks=callbacks,
        metric=metric)

    if not runner.resumed:
        for exp in experiments:
            search_alg.add_configurations([exp])
    else:
        logger.info("TrialRunner resumed, ignoring new add_experiment.")

    if progress_reporter is None:
        if IS_NOTEBOOK:
            progress_reporter = JupyterNotebookReporter(
                overwrite=not has_verbosity(Verbosity.V2_TRIAL_NORM))
        else:
            progress_reporter = CLIReporter()

    if not progress_reporter.set_search_properties(metric, mode):
        raise ValueError(
            "You passed a `metric` or `mode` argument to `tune.run()`, but "
            "the reporter you are using was already instantiated with their "
            "own `metric` and `mode` parameters. Either remove the arguments "
            "from your reporter or from your call to `tune.run()`")
    progress_reporter.set_total_samples(search_alg.total_samples)

    # User Warning for GPUs
    if trial_executor.has_gpus():
        if isinstance(resources_per_trial,
                      dict) and "gpu" in resources_per_trial:
            # "gpu" is manually set.
            pass
        elif _check_default_resources_override(experiments[0].run_identifier):
            # "default_resources" is manually overridden.
            pass
        else:
            logger.warning("Tune detects GPUs, but no trials are using GPUs. "
                           "To enable trials to use GPUs, set "
                           "tune.run(resources_per_trial={'gpu': 1}...) "
                           "which allows Tune to expose 1 GPU to each trial. "
                           "You can also override "
                           "`Trainable.default_resource_request` if using the "
                           "Trainable API.")

    tune_start = time.time()
    while not runner.is_finished():
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
    runner.cleanup_trials()

    incomplete_trials = []
    for trial in runner.get_trials():
        if trial.status != Trial.TERMINATED:
            incomplete_trials += [trial]

    if incomplete_trials:
        if raise_on_failed_trial:
            raise TuneError("Trials did not complete", incomplete_trials)
        else:
            logger.error("Trials did not complete: %s", incomplete_trials)

    all_taken = time.time() - all_start
    if has_verbosity(Verbosity.V1_EXPERIMENT):
        logger.info(f"Total run time: {all_taken:.2f} seconds "
                    f"({tune_taken:.2f} seconds for the tuning loop).")

    trials = runner.get_trials()
    return ExperimentAnalysis(
        runner.checkpoint_file,
        trials=trials,
        default_metric=metric,
        default_mode=mode)


def run_experiments(experiments,
                    scheduler=None,
                    server_port=None,
                    verbose=Verbosity.V3_TRIAL_DETAILS,
                    progress_reporter=None,
                    resume=False,
                    queue_trials=False,
                    reuse_actors=False,
                    trial_executor=None,
                    raise_on_failed_trial=True,
                    concurrent=True,
                    callbacks=None):
    """Runs and blocks until all trials finish.

    Examples:
        >>> experiment_spec = Experiment("experiment", my_func)
        >>> run_experiments(experiments=experiment_spec)

        >>> experiment_spec = {"experiment": {"run": my_func}}
        >>> run_experiments(experiments=experiment_spec)

    Returns:
        List of Trial objects, holding data for each executed trial.

    """
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
            queue_trials=queue_trials,
            reuse_actors=reuse_actors,
            trial_executor=trial_executor,
            raise_on_failed_trial=raise_on_failed_trial,
            scheduler=scheduler,
            callbacks=callbacks).trials
    else:
        trials = []
        for exp in experiments:
            trials += run(
                exp,
                server_port=server_port,
                verbose=verbose,
                progress_reporter=progress_reporter,
                resume=resume,
                queue_trials=queue_trials,
                reuse_actors=reuse_actors,
                trial_executor=trial_executor,
                raise_on_failed_trial=raise_on_failed_trial,
                scheduler=scheduler,
                callbacks=callbacks).trials
        return trials

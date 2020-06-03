import logging

from ray.tune.error import TuneError
from ray.tune.experiment import convert_to_experiment_list, Experiment
from ray.tune.analysis import ExperimentAnalysis
from ray.tune.suggest import BasicVariantGenerator
from ray.tune.suggest.suggestion import Searcher, SearchGenerator
from ray.tune.trial import Trial
from ray.tune.trainable import Trainable
from ray.tune.ray_trial_executor import RayTrialExecutor
from ray.tune.registry import get_trainable_cls
from ray.tune.syncer import wait_for_sync
from ray.tune.trial_runner import TrialRunner
from ray.tune.progress_reporter import CLIReporter, JupyterNotebookReporter
from ray.tune.schedulers import (HyperBandScheduler, AsyncHyperBandScheduler,
                                 FIFOScheduler, MedianStoppingRule)
from ray.tune.web_server import TuneServer

logger = logging.getLogger(__name__)

_SCHEDULERS = {
    "FIFO": FIFOScheduler,
    "MedianStopping": MedianStoppingRule,
    "HyperBand": HyperBandScheduler,
    "AsyncHyperBand": AsyncHyperBandScheduler,
}

try:
    class_name = get_ipython().__class__.__name__
    IS_NOTEBOOK = True if "Terminal" not in class_name else False
except NameError:
    IS_NOTEBOOK = False


def _make_scheduler(args):
    if args.scheduler in _SCHEDULERS:
        return _SCHEDULERS[args.scheduler](**args.scheduler_config)
    else:
        raise TuneError("Unknown scheduler: {}, should be one of {}".format(
            args.scheduler, _SCHEDULERS.keys()))


def _check_default_resources_override(run_identifier):
    if not isinstance(run_identifier, str):
        # If obscure dtype, assume it is overriden.
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


def run(run_or_experiment,
        name=None,
        stop=None,
        config=None,
        resources_per_trial=None,
        num_samples=1,
        local_dir=None,
        upload_dir=None,
        trial_name_creator=None,
        loggers=None,
        sync_to_cloud=None,
        sync_to_driver=None,
        checkpoint_freq=0,
        checkpoint_at_end=False,
        sync_on_checkpoint=True,
        keep_checkpoints_num=None,
        checkpoint_score_attr=None,
        global_checkpoint_period=10,
        export_formats=None,
        max_failures=0,
        fail_fast=False,
        restore=None,
        search_alg=None,
        scheduler=None,
        with_server=False,
        server_port=TuneServer.DEFAULT_PORT,
        verbose=2,
        progress_reporter=None,
        resume=False,
        queue_trials=False,
        reuse_actors=False,
        trial_executor=None,
        raise_on_failed_trial=True,
        return_trials=False,
        ray_auto_init=True):
    """Executes training.

    Args:
        run_or_experiment (function | class | str | :class:`Experiment`): If
            function|class|str, this is the algorithm or model to train.
            This may refer to the name of a built-on algorithm
            (e.g. RLLib's DQN or PPO), a user-defined trainable
            function or class, or the string identifier of a
            trainable function or class registered in the tune registry.
            If Experiment, then Tune will execute training based on
            Experiment.spec.
        name (str): Name of experiment.
        stop (dict | callable | :class:`Stopper`): Stopping criteria. If dict,
            the keys may be any field in the return result of 'train()',
            whichever is reached first. If function, it must take (trial_id,
            result) as arguments and return a boolean (True if trial should be
            stopped, False otherwise). This can also be a subclass of
            ``ray.tune.Stopper``, which allows users to implement
            custom experiment-wide stopping (i.e., stopping an entire Tune
            run based on some time constraint).
        config (dict): Algorithm-specific configuration for Tune variant
            generation (e.g. env, hyperparams). Defaults to empty dict.
            Custom search algorithms may ignore this.
        resources_per_trial (dict): Machine resources to allocate per trial,
            e.g. ``{"cpu": 64, "gpu": 8}``. Note that GPUs will not be
            assigned unless you specify them here. Defaults to 1 CPU and 0
            GPUs in ``Trainable.default_resource_request()``.
        num_samples (int): Number of times to sample from the
            hyperparameter space. Defaults to 1. If `grid_search` is
            provided as an argument, the grid will be repeated
            `num_samples` of times.
        local_dir (str): Local dir to save training results to.
            Defaults to ``~/ray_results``.
        upload_dir (str): Optional URI to sync training results and checkpoints
            to (e.g. ``s3://bucket`` or ``gs://bucket``).
        trial_name_creator (func): Optional function for generating
            the trial string representation.
        loggers (list): List of logger creators to be used with
            each Trial. If None, defaults to ray.tune.logger.DEFAULT_LOGGERS.
            See `ray/tune/logger.py`.
        sync_to_cloud (func|str): Function for syncing the local_dir to and
            from upload_dir. If string, then it must be a string template that
            includes `{source}` and `{target}` for the syncer to run. If not
            provided, the sync command defaults to standard S3 or gsutil sync
            commands. By default local_dir is synced to remote_dir every 300
            seconds. To change this, set the TUNE_CLOUD_SYNC_S
            environment variable in the driver machine.
        sync_to_driver (func|str|bool): Function for syncing trial logdir from
            remote node to local. If string, then it must be a string template
            that includes `{source}` and `{target}` for the syncer to run.
            If True or not provided, it defaults to using rsync. If False,
            syncing to driver is disabled.
        checkpoint_freq (int): How many training iterations between
            checkpoints. A value of 0 (default) disables checkpointing.
        checkpoint_at_end (bool): Whether to checkpoint at the end of the
            experiment regardless of the checkpoint_freq. Default is False.
        sync_on_checkpoint (bool): Force sync-down of trial checkpoint to
            driver. If set to False, checkpoint syncing from worker to driver
            is asynchronous and best-effort. This does not affect persistent
            storage syncing. Defaults to True.
        keep_checkpoints_num (int): Number of checkpoints to keep. A value of
            `None` keeps all checkpoints. Defaults to `None`. If set, need
            to provide `checkpoint_score_attr`.
        checkpoint_score_attr (str): Specifies by which attribute to rank the
            best checkpoint. Default is increasing order. If attribute starts
            with `min-` it will rank attribute in decreasing order, i.e.
            `min-validation_loss`.
        global_checkpoint_period (int): Seconds between global checkpointing.
            This does not affect `checkpoint_freq`, which specifies frequency
            for individual trials.
        export_formats (list): List of formats that exported at the end of
            the experiment. Default is None.
        max_failures (int): Try to recover a trial at least this many times.
            Ray will recover from the latest checkpoint if present.
            Setting to -1 will lead to infinite recovery retries.
            Setting to 0 will disable retries. Defaults to 3.
        fail_fast (bool): Whether to fail upon the first error.
        restore (str): Path to checkpoint. Only makes sense to set if
            running 1 trial. Defaults to None.
        search_alg (Searcher): Search algorithm for optimization.
        scheduler (TrialScheduler): Scheduler for executing
            the experiment. Choose among FIFO (default), MedianStopping,
            AsyncHyperBand, HyperBand and PopulationBasedTraining. Refer to
            ray.tune.schedulers for more options.
        with_server (bool): Starts a background Tune server. Needed for
            using the Client API.
        server_port (int): Port number for launching TuneServer.
        verbose (int): 0, 1, or 2. Verbosity mode. 0 = silent,
            1 = only status updates, 2 = status and trial results.
        progress_reporter (ProgressReporter): Progress reporter for reporting
            intermediate experiment progress. Defaults to CLIReporter if
            running in command-line, or JupyterNotebookReporter if running in
            a Jupyter notebook.
        resume (str|bool): One of "LOCAL", "REMOTE", "PROMPT", or bool.
            LOCAL/True restores the checkpoint from the local_checkpoint_dir.
            REMOTE restores the checkpoint from remote_checkpoint_dir.
            PROMPT provides CLI feedback. False forces a new
            experiment. If resume is set but checkpoint does not exist,
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
        ray_auto_init (bool): Automatically starts a local Ray cluster
            if using a RayTrialExecutor (which is the default) and
            if Ray is not initialized. Defaults to True.

    Returns:
        ExperimentAnalysis: Object for experiment analysis.

    Raises:
        TuneError: Any trials failed and `raise_on_failed_trial` is True.

    Examples:

    .. code-block:: python

        # Run 10 trials (each trial is one instance of a Trainable). Tune runs
        # in parallel and automatically determines concurrency.
        tune.run(trainable, num_samples=10)

        # Run 1 trial, stop when trial has reached 10 iterations
        tune.run(my_trainable, stop={"training_iteration": 10})

        # Run 1 trial, search over hyperparameters, stop after 10 iterations.
        space = {"lr": tune.uniform(0, 1), "momentum": tune.uniform(0, 1)}
        tune.run(my_trainable, config=space, stop={"training_iteration": 10})
    """
    trial_executor = trial_executor or RayTrialExecutor(
        queue_trials=queue_trials,
        reuse_actors=reuse_actors,
        ray_auto_init=ray_auto_init)
    if isinstance(run_or_experiment, list):
        experiments = run_or_experiment
    else:
        experiments = [run_or_experiment]

    for i, exp in enumerate(experiments):
        if not isinstance(exp, Experiment):
            run_identifier = Experiment.register_if_needed(exp)
            experiments[i] = Experiment(
                name=name,
                run=run_identifier,
                stop=stop,
                config=config,
                resources_per_trial=resources_per_trial,
                num_samples=num_samples,
                local_dir=local_dir,
                upload_dir=upload_dir,
                sync_to_driver=sync_to_driver,
                trial_name_creator=trial_name_creator,
                loggers=loggers,
                checkpoint_freq=checkpoint_freq,
                checkpoint_at_end=checkpoint_at_end,
                sync_on_checkpoint=sync_on_checkpoint,
                keep_checkpoints_num=keep_checkpoints_num,
                checkpoint_score_attr=checkpoint_score_attr,
                export_formats=export_formats,
                max_failures=max_failures,
                restore=restore)
    else:
        logger.debug("Ignoring some parameters passed into tune.run.")

    if sync_to_cloud:
        for exp in experiments:
            assert exp.remote_checkpoint_dir, (
                "Need `upload_dir` if `sync_to_cloud` given.")

    if fail_fast and max_failures != 0:
        raise ValueError("max_failures must be 0 if fail_fast=True.")

    if issubclass(type(search_alg), Searcher):
        search_alg = SearchGenerator(search_alg)

    runner = TrialRunner(
        search_alg=search_alg or BasicVariantGenerator(),
        scheduler=scheduler or FIFOScheduler(),
        local_checkpoint_dir=experiments[0].checkpoint_dir,
        remote_checkpoint_dir=experiments[0].remote_checkpoint_dir,
        sync_to_cloud=sync_to_cloud,
        stopper=experiments[0].stopper,
        checkpoint_period=global_checkpoint_period,
        resume=resume,
        launch_web_server=with_server,
        server_port=server_port,
        verbose=bool(verbose > 1),
        fail_fast=fail_fast,
        trial_executor=trial_executor)

    for exp in experiments:
        runner.add_experiment(exp)

    if progress_reporter is None:
        if IS_NOTEBOOK:
            progress_reporter = JupyterNotebookReporter(overwrite=verbose < 2)
        else:
            progress_reporter = CLIReporter()

    # User Warning for GPUs
    if trial_executor.has_gpus():
        if isinstance(resources_per_trial,
                      dict) and "gpu" in resources_per_trial:
            # "gpu" is manually set.
            pass
        elif _check_default_resources_override(experiments[0].run_identifier):
            # "default_resources" is manually overriden.
            pass
        else:
            logger.warning("Tune detects GPUs, but no trials are using GPUs. "
                           "To enable trials to use GPUs, set "
                           "tune.run(resources_per_trial={'gpu': 1}...) "
                           "which allows Tune to expose 1 GPU to each trial. "
                           "You can also override "
                           "`Trainable.default_resource_request` if using the "
                           "Trainable API.")

    while not runner.is_finished():
        runner.step()
        if verbose:
            _report_progress(runner, progress_reporter)

    try:
        runner.checkpoint(force=True)
    except Exception:
        logger.exception("Trial Runner checkpointing failed.")

    if verbose:
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

    trials = runner.get_trials()
    if return_trials:
        return trials
    return ExperimentAnalysis(runner.checkpoint_file, trials=trials)


def run_experiments(experiments,
                    search_alg=None,
                    scheduler=None,
                    with_server=False,
                    server_port=TuneServer.DEFAULT_PORT,
                    verbose=2,
                    progress_reporter=None,
                    resume=False,
                    queue_trials=False,
                    reuse_actors=False,
                    trial_executor=None,
                    raise_on_failed_trial=True,
                    concurrent=True):
    """Runs and blocks until all trials finish.

    Examples:
        >>> experiment_spec = Experiment("experiment", my_func)
        >>> run_experiments(experiments=experiment_spec)

        >>> experiment_spec = {"experiment": {"run": my_func}}
        >>> run_experiments(experiments=experiment_spec)

        >>> run_experiments(
        >>>     experiments=experiment_spec,
        >>>     scheduler=MedianStoppingRule(...))

        >>> run_experiments(
        >>>     experiments=experiment_spec,
        >>>     search_alg=SearchAlgorithm(),
        >>>     scheduler=MedianStoppingRule(...))

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
            search_alg=search_alg,
            scheduler=scheduler,
            with_server=with_server,
            server_port=server_port,
            verbose=verbose,
            progress_reporter=progress_reporter,
            resume=resume,
            queue_trials=queue_trials,
            reuse_actors=reuse_actors,
            trial_executor=trial_executor,
            raise_on_failed_trial=raise_on_failed_trial,
            return_trials=True)
    else:
        trials = []
        for exp in experiments:
            trials += run(
                exp,
                search_alg=search_alg,
                scheduler=scheduler,
                with_server=with_server,
                server_port=server_port,
                verbose=verbose,
                progress_reporter=progress_reporter,
                resume=resume,
                queue_trials=queue_trials,
                reuse_actors=reuse_actors,
                trial_executor=trial_executor,
                raise_on_failed_trial=raise_on_failed_trial,
                return_trials=True)
        return trials

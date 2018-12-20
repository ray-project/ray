from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import os
import time

from ray.tune.error import TuneError
from ray.tune.experiment import convert_to_experiment_list
from ray.tune.suggest import BasicVariantGenerator
from ray.tune.trial import Trial, DEBUG_PRINT_INTERVAL
from ray.tune.log_sync import wait_for_log_sync
from ray.tune.trial_runner import TrialRunner
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


def _make_scheduler(args):
    if args.scheduler in _SCHEDULERS:
        return _SCHEDULERS[args.scheduler](**args.scheduler_config)
    else:
        raise TuneError("Unknown scheduler: {}, should be one of {}".format(
            args.scheduler, _SCHEDULERS.keys()))


def _find_checkpoint_dir(exp_list):
    checkpointable_expts = [exp for exp in exp_list if exp.is_checkpointable()]
    logger.info("Searching checkpointable experiments for checkpoint_dir.")
    if checkpointable_expts:
        return checkpointable_expts[0].spec["local_dir"]
    else:
        return None


def run_experiments(experiments=None,
                    search_alg=None,
                    scheduler=None,
                    with_server=False,
                    server_port=TuneServer.DEFAULT_PORT,
                    verbose=True,
                    resume=False,
                    checkpoint_mode=False,
                    queue_trials=False,
                    trial_executor=None,
                    raise_on_failed_trial=True):
    """Runs and blocks until all trials finish.

    Args:
        experiments (Experiment | list | dict): Experiments to run. Will be
            passed to `search_alg` via `add_configurations`.
        search_alg (SearchAlgorithm): Search Algorithm. Defaults to
            BasicVariantGenerator.
        scheduler (TrialScheduler): Scheduler for executing
            the experiment. Choose among FIFO (default), MedianStopping,
            AsyncHyperBand, and HyperBand.
        checkpoint_dir (str): Path at which experiment checkpoints are stored
            and restored from.
        with_server (bool): Starts a background Tune server. Needed for
            using the Client API.
        server_port (int): Port number for launching TuneServer.
        verbose (bool): How much output should be printed for each trial.
        resume (bool): To resume from full experiment checkpoint. Only the
            first checkpointable experiment local_dir is checked.
            If checkpoint exists, the experiment will resume from there.
            Requires `checkpoint_mode` to be True (even during resume).
        checkpoint_mode: Turns on checkpointing for full Tune experiment.
            Currently defaults to False.
        queue_trials (bool): Whether to queue trials when the cluster does
            not currently have enough resources to launch one. This should
            be set to True when running on an autoscaling cluster to enable
            automatic scale-up.
        trial_executor (TrialExecutor): Manage the execution of trials.
        raise_on_failed_trial (bool): Raise TuneError if there exists failed
            trial (of ERROR state) when the experiments complete.

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
    checkpoint_dir = _find_checkpoint_dir(experiments)

    runner = None

    if resume:
        if not checkpoint_mode:
            raise ValueError("Resuming Tune experiment run "
                             "requires `checkpoint_mode`.")
        if not checkpoint_dir:
            raise ValueError("Did not find a checkpoint_dir. "
                             "Do any experiments have checkpointing on?")
        logger.info("Using checkpoint_dir: {}.".format(checkpoint_dir))
        if not os.path.exists(
                os.path.join(checkpoint_dir, TrialRunner.CKPT_FILE)):
            raise ValueError(
                "Did not find checkpoint file in {}.".format(checkpoint_dir))

        logger.warn("Restoring from previous experiment and "
                    "ignoring any new changes to specification.")
        runner = TrialRunner.restore(checkpoint_dir, trial_executor)

    if not runner:
        if scheduler is None:
            scheduler = FIFOScheduler()

        if search_alg is None:
            search_alg = BasicVariantGenerator()

        search_alg.add_configurations(experiments)

        runner = TrialRunner(
            search_alg,
            scheduler=scheduler,
            checkpoint_dir=checkpoint_dir,
            checkpoint_mode=checkpoint_mode,
            launch_web_server=with_server,
            server_port=server_port,
            verbose=verbose,
            queue_trials=queue_trials,
            trial_executor=trial_executor)

    logger.info(runner.debug_string(max_debug=99999))
    last_debug = 0
    while not runner.is_finished():
        runner.step()
        if time.time() - last_debug > DEBUG_PRINT_INTERVAL:
            logger.info(runner.debug_string())
            last_debug = time.time()

    logger.info(runner.debug_string(max_debug=99999))

    wait_for_log_sync()

    errored_trials = []
    for trial in runner.get_trials():
        if trial.status != Trial.TERMINATED:
            errored_trials += [trial]

    if errored_trials:
        if raise_on_failed_trial:
            raise TuneError("Trials did not complete", errored_trials)
        else:
            logger.error("Trials did not complete: %s", errored_trials)

    return runner.get_trials()

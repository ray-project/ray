from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import time

from ray.tune.error import TuneError
from ray.tune.hyperband import HyperBandScheduler
from ray.tune.async_hyperband import AsyncHyperBandScheduler
from ray.tune.median_stopping_rule import MedianStoppingRule
from ray.tune.hpo_scheduler import HyperOptScheduler
from ray.tune.trial import Trial, DEBUG_PRINT_INTERVAL
from ray.tune.log_sync import wait_for_log_sync
from ray.tune.trial_runner import TrialRunner
from ray.tune.trial_scheduler import FIFOScheduler
from ray.tune.web_server import TuneServer
from ray.tune.experiment import Experiment


_SCHEDULERS = {
    "FIFO": FIFOScheduler,
    "MedianStopping": MedianStoppingRule,
    "HyperBand": HyperBandScheduler,
    "AsyncHyperBand": AsyncHyperBandScheduler,
    "HyperOpt": HyperOptScheduler,
}


def _make_scheduler(args):
    if args.scheduler in _SCHEDULERS:
        return _SCHEDULERS[args.scheduler](**args.scheduler_config)
    else:
        raise TuneError(
            "Unknown scheduler: {}, should be one of {}".format(
                args.scheduler, _SCHEDULERS.keys()))


def run_experiments(experiments, scheduler=None, with_server=False,
                    server_port=TuneServer.DEFAULT_PORT, verbose=True):
    """Tunes experiments.

    Args:
        experiments (Experiment | list | dict): Experiments to run.
        scheduler (TrialScheduler): Scheduler for executing
            the experiment. Choose among FIFO (default), MedianStopping,
            AsyncHyperBand, HyperBand, or HyperOpt.
        with_server (bool): Starts a background Tune server. Needed for
            using the Client API.
        server_port (int): Port number for launching TuneServer.
        verbose (bool): How much output should be printed for each trial.
    """

    if scheduler is None:
        scheduler = FIFOScheduler()

    runner = TrialRunner(
        scheduler, launch_web_server=with_server, server_port=server_port,
        verbose=verbose)
    exp_list = experiments
    if isinstance(experiments, Experiment):
        exp_list = [experiments]
    elif type(experiments) is dict:
        exp_list = [Experiment.from_json(name, spec)
                    for name, spec in experiments.items()]

    if (type(exp_list) is list and
            all(isinstance(exp, Experiment) for exp in exp_list)):
        for experiment in exp_list:
            scheduler.add_experiment(experiment, runner)
    else:
        raise TuneError("Invalid argument: {}".format(experiments))

    print(runner.debug_string(max_debug=99999))

    last_debug = 0
    while not runner.is_finished():
        runner.step()
        if time.time() - last_debug > DEBUG_PRINT_INTERVAL:
            print(runner.debug_string())
            last_debug = time.time()

    print(runner.debug_string(max_debug=99999))

    for trial in runner.get_trials():
        # TODO(rliaw): What about errored?
        if trial.status != Trial.TERMINATED:
            raise TuneError("Trial did not complete", trial)

    wait_for_log_sync()
    return runner.get_trials()

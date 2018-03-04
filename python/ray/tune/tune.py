from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import time

from ray.tune import TuneError
from ray.tune.hyperband import HyperBandScheduler
from ray.tune.async_hyperband import AsyncHyperBandScheduler
from ray.tune.median_stopping_rule import MedianStoppingRule
from ray.tune.trial import Trial, DEBUG_PRINT_INTERVAL
from ray.tune.log_sync import wait_for_log_sync
from ray.tune.trial_runner import TrialRunner
from ray.tune.trial_scheduler import FIFOScheduler
from ray.tune.web_server import TuneServer
from ray.tune.variant_generator import generate_trials


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
        raise TuneError(
            "Unknown scheduler: {}, should be one of {}".format(
                args.scheduler, _SCHEDULERS.keys()))


def run_experiments(experiments, scheduler=None, with_server=False,
                    server_port=TuneServer.DEFAULT_PORT, verbose=True):

    # Make sure rllib agents are registered
    from ray import rllib  # noqa # pylint: disable=unused-import

    if scheduler is None:
        scheduler = FIFOScheduler()

    runner = TrialRunner(
        scheduler, launch_web_server=with_server, server_port=server_port)

    for name, spec in experiments.items():
        for trial in generate_trials(spec, name):
            trial.set_verbose(verbose)
            runner.add_trial(trial)
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

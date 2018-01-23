from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import time

from ray.tune import TuneError
from ray.tune.hyperband import HyperBandScheduler
from ray.tune.median_stopping_rule import MedianStoppingRule
from ray.tune.trial import Trial, DEBUG_PRINT_INTERVAL
from ray.tune.trial_runner import TrialRunner
from ray.tune.trial_scheduler import FIFOScheduler
from ray.tune.variant_generator import generate_trials


_SCHEDULERS = {
    "FIFO": FIFOScheduler,
    "MedianStopping": MedianStoppingRule,
    "HyperBand": HyperBandScheduler,
}


def _make_scheduler(args):
    if args.scheduler in _SCHEDULERS:
        return _SCHEDULERS[args.scheduler](**args.scheduler_config)
    else:
        raise TuneError(
            "Unknown scheduler: {}, should be one of {}".format(
                args.scheduler, _SCHEDULERS.keys()))


def run_experiments(experiments, scheduler=None):
    if scheduler is None:
        scheduler = FIFOScheduler()
    runner = TrialRunner(scheduler)

    for name, spec in experiments.items():
        for trial in generate_trials(spec, name):
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
        if trial.status != Trial.TERMINATED:
            raise TuneError("Trial did not complete", trial)

    return runner.get_trials()

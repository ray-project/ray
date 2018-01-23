from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import time

from ray.tune import TuneError
from ray.tune.trial import Trial
from ray.tune.trial_runner import TrialRunner
from ray.tune.trial_scheduler import FIFOScheduler
from ray.tune.variant_generator import generate_trials


def run_experiments(experiments, scheduler=None, verbose=True):
    if scheduler is None:
        scheduler = FIFOScheduler()
    runner = TrialRunner(scheduler, verbose)

    for name, spec in experiments.items():
        for trial in generate_trials(spec, name):
            runner.add_trial(trial)
    print(runner.verbose_debug_string())

    last_debug = time.time()
    while not runner.is_finished():
        runner.step()
        if verbose or time.time() - last_debug > 5:
            print(runner.debug_string())
            last_debug = time.time()

    print(runner.verbose_debug_string())

    for trial in runner.get_trials():
        if trial.status != Trial.TERMINATED:
            raise TuneError("Trial did not complete", trial)

    return runner.get_trials()

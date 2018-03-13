from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import time

from ray.tune import TuneError
from ray.tune.hyperband import HyperBandScheduler
from ray.tune.async_hyperband import AsyncHyperBandScheduler
from ray.tune.median_stopping_rule import MedianStoppingRule
from ray.tune.trial import Trial, DEBUG_PRINT_INTERVAL
from ray.tune.result import DEFAULT_RESULTS_DIR
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


class Experiment():
    def __init__(self, name, run, stop, config,
                 resources=None, repeat=1, local_dir=DEFAULT_RESULTS_DIR,
                 upload_dir="", checkpoint_freq=0, max_failures=3):
        """Initializes object to track experiment specs.

        Args:
            name (str): Name of experiment.
            run (str): The algorithm or model to train. This may refer to the
                name of a built-on algorithm (e.g. RLLib’s DQN or PPO), or a
                user-defined trainable function or class
                registered in the tune registry.
            stop (dict): The stopping criteria. The keys may be any field in
                TrainingResult, whichever is reached first.
            config (dict): Algorithm-specific configuration
                (e.g. env, hyperparams).
            resources (dict): Machine resources to allocate per trial,
                e.g. {“cpu”: 64, “gpu”: 8}. Note that GPUs will not be
                assigned unless you specify them here. Defaults to 1 CPU and 0
                GPUs.
            repeat (int): Number of times to repeat each trial. Defaults to 1.
            local_dir (str): Local dir to save training results to.
                Defaults to `~/ray_results`.
            upload_dir (str): Optional URI to sync training results
                to (e.g. s3://bucket).
            checkpoint_freq (int): How many training iterations between
                checkpoints. A value of 0 (default) disables checkpointing.
            max_failures (int): Try to recover a trial from its last
                checkpoint at least this many times. Only applies if
                checkpointing is enabled. Defaults to 3.
        """
        spec = {
            "run": run,
            "stop": stop,
            "config": config,
            "resources": resources,
            "repeat": repeat,
            "local_dir": local_dir,
            "upload_dir": upload_dir,
            "checkpoint_freq": checkpoint_freq,
            "max_failures": max_failures
        }
        self._trials = generate_trials(spec, name)

    def trials(self):
        for trial in self._trials:
            yield trial


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

    if type(experiments) is dict:
        for name, spec in experiments.items():
            for trial in generate_trials(spec, name):
                trial.set_verbose(verbose)
                runner.add_trial(trial)
    elif (type(experiments) is list and
          all(isinstance(exp, Experiment) for exp in experiments)):
        for experiment in experiments:
            for trial in experiments.trials():
                trial.set_verbose(verbose)
                runner.add_trial(trial)
    elif isinstance(experiments, Experiment):
        for trial in experiments.trials():
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


if __name__ == '__main__':
    run_experiments()

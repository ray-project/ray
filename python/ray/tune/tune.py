#!/usr/bin/env python

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import json
import sys
import yaml

import ray
from ray.tune.hyperband import HyperBandScheduler
from ray.tune.median_stopping_rule import MedianStoppingRule
from ray.tune.trial import Trial
from ray.tune.trial_runner import TrialRunner
from ray.tune.trial_scheduler import FIFOScheduler
from ray.tune.variant_generator import generate_trials


EXAMPLE_USAGE = """
MNIST tuning example:
    ./tune.py -f examples/tune_mnist_ray.yaml
"""


parser = argparse.ArgumentParser(
    formatter_class=argparse.RawDescriptionHelpFormatter,
    description="Tune hyperparameters with Ray.",
    epilog=EXAMPLE_USAGE)

# See also the base parser definition in ray/tune/config_parser.py
parser.add_argument("--redis-address", default=None, type=str,
                    help="The Redis address of the cluster.")
parser.add_argument("--num-cpus", default=None, type=int,
                    help="Number of CPUs to allocate to Ray.")
parser.add_argument("--num-gpus", default=None, type=int,
                    help="Number of GPUs to allocate to Ray.")
parser.add_argument("--scheduler", default="FIFO", type=str,
                    help="FIFO, MedianStopping, or HyperBand")
parser.add_argument("--scheduler-config", default="{}", type=json.loads,
                    help="Config options to pass to the scheduler.")
parser.add_argument("-f", "--config-file", required=True, type=str,
                    help="Read experiment options from this JSON/YAML file.")


SCHEDULERS = {
    "FIFO": FIFOScheduler,
    "MedianStopping": MedianStoppingRule,
    "HyperBand": HyperBandScheduler,
}


def make_scheduler(args):
    if args.scheduler in SCHEDULERS:
        return SCHEDULERS[args.scheduler](**args.scheduler_config)
    else:
        assert False, "Unknown scheduler: {}, should be one of {}".format(
            args.scheduler, SCHEDULERS.keys())


def run_experiments(experiments, scheduler=None, **ray_args):
    if scheduler is None:
        scheduler = make_scheduler(args)
    runner = TrialRunner(scheduler)

    for name, spec in experiments.items():
        for trial in generate_trials(spec, name):
            runner.add_trial(trial)
    print(runner.debug_string())

    ray.init(**ray_args)

    while not runner.is_finished():
        runner.step()
        print(runner.debug_string())

    for trial in runner.get_trials():
        if trial.status != Trial.TERMINATED:
            print("Exit 1")
            sys.exit(1)

    print("Exit 0")


if __name__ == "__main__":
    args = parser.parse_args(sys.argv[1:])
    with open(args.config_file) as f:
        experiments = yaml.load(f)
    run_experiments(
        experiments, make_scheduler(args), redis_address=args.redis_address,
        num_cpus=args.num_cpus, num_gpus=args.num_gpus)

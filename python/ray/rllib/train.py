#!/usr/bin/env python

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import sys
import yaml

import ray
from ray.tune.config_parser import make_parser, resources_to_json
from ray.tune.trial_runner import TrialRunner
from ray.tune.trial import Trial
from ray.tune.tune import run_experiments
from ray.tune.variant_generator import spec_to_trials


EXAMPLE_USAGE = """
Training example:
    ./train.py --alg DQN --env CartPole-v0

Grid search example:
    ./train.py -f tuned_examples/cartpole-grid-search-example.yaml
"""


parser = make_parser(
    formatter_class=argparse.RawDescriptionHelpFormatter,
    description="Train a reinforcement learning agent.",
    epilog=EXAMPLE_USAGE)

# See also the base parser definition in ray/tune/config_parser.py
parser.add_argument("--redis-address", default=None, type=str,
                    help="The Redis address of the cluster.")
parser.add_argument("--num-cpus", default=None, type=int,
                    help="Number of CPUs to allocate to Ray.")
parser.add_argument("--num-gpus", default=None, type=int,
                    help="Number of GPUs to allocate to Ray.")
parser.add_argument("-f", "--config-file", default=None, type=str,
                    help="If specified, use config options from this file.")


def main(argv):
    args = parser.parse_args(argv)
    runner = TrialRunner()

    if args.config_file:
        with open(args.config_file) as f:
            experiments = yaml.load(f)
    else:
        missing_args = []
        if not args.alg:
            missing_args.append("--alg")
        if not args.env:
            missing_args.append("--env")
        if missing_args:
            parser.error(
                "the following arguments are required: {}".format(
                    " ".join(missing_args)))
        experiments = {
            "": {
                "alg": args.alg,
                "env": args.env,
                "resources": resources_to_json(args.resources),
                "stop": args.stop,
                "config": args.config,
                "restore": args.restore,
                "repeat": args.repeat,
            }
        }

    for name, spec in experiments.items():
        for trial in spec_to_trials(spec, name):
            runner.add_trial(trial)
    print(runner.debug_string())

    ray.init(
        redis_address=args.redis_address, num_cpus=args.num_cpus,
        num_gpus=args.num_gpus)

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
    if args.config_file:
        with open(args.config_file) as f:
            experiments = yaml.load(f)
    else:
        missing_args = []
        if not args.alg:
            missing_args.append("--alg")
        if not args.env:
            missing_args.append("--env")
        if missing_args:
            parser.error(
                "the following arguments are required: {}".format(
                    " ".join(missing_args)))
        experiments = {
            '': {
                "alg": args.alg,
                "env": args.env,
                "resources": resources_to_json(args.resources),
                "stop": args.stop,
                "config": args.config,
                "restore": args.restore,
            }
        }
    run_experiments(
        experiments, redis_address=args.redis_address,
        num_cpus=args.num_cpus, num_gpus=args.num_gpus)

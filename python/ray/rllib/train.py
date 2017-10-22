#!/usr/bin/env python

"""The main command line interface to RLlib.

Arguments may either be specified on the command line or in JSON/YAML
files. Additionally, the file-based interface supports hyperparameter
exploration through grid or random search, though both interfaces allow
for the concurrent execution of multiple trials on Ray.

Single-trial example:
    ./train.py --alg=DQN --env=CartPole-v0

Hyperparameter grid search example:
    ./train.py -f tuned_examples/cartpole-grid-search-example.yaml
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import sys
import yaml

import ray
from ray.tune.config_parser import make_parser
from ray.tune.trial_runner import TrialRunner
from ray.tune.trial import Trial
from ray.tune.variant_generator import spec_to_trials


parser = make_parser("Train a reinforcement learning agent.")

# Extends the base parser defined in ray/tune/config_parser, to add some
# RLlib specific arguments. For more arguments, see the configuration
# defined there.
parser.add_argument("--redis-address", default=None, type=str,
                    help="The Redis address of the cluster.")
parser.add_argument("--num-cpus", default=None, type=int,
                    help="Number of CPUs to allocate to Ray.")
parser.add_argument("--num-gpus", default=None, type=int,
                    help="Number of GPUs to allocate to Ray.")
parser.add_argument("--restore", default=None, type=str,
                    help="If specified, restore from this checkpoint.")
parser.add_argument("--experiment-name", default='', type=str,
                    help="Optional name to give this experiment")
parser.add_argument("-f", "--config-file", default=None, type=str,
                    help="If specified, use config options from this file.")


def main(argv):
    args = parser.parse_args(argv)
    runner = TrialRunner()

    if args.config_file:
        with open(args.config_file) as f:
            json_spec = yaml.load(f)
    else:
        json_spec = {
            "alg": args.alg,
            "env": args.env,
            "resources": args.resources,
            "stop": args.stop,
            "config": args.config,
        }

    for trial in spec_to_trials(json_spec, args.experiment_name):
        runner.add_trial(trial)

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
    main(sys.argv[1:])

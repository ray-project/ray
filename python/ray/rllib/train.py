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
from ray.tune.config_parser import make_parser, parse_to_trials
from ray.tune.trial_runner import TrialRunner
from ray.tune.trial import Trial


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
parser.add_argument("-f", "--config-file", default=None, type=str,
                    help="If specified, use config options from this file.")


def main(argv):
    args = parser.parse_args(argv)
    runner = TrialRunner()

    if args.config_file:
        with open(args.config_file) as f:
            config = yaml.load(f)
        for trial in parse_to_trials(config):
            runner.add_trial(trial)
    else:
        runner.add_trial(
            Trial(
                args.env, args.alg, args.config, args.local_dir, None,
                args.resources, args.stop, args.checkpoint_freq, args.restore,
                args.upload_dir))
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

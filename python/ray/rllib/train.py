#!/usr/bin/env python

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import sys
import yaml

from ray.tune.config_parser import make_parser, resources_to_json
from ray.tune.tune import make_scheduler, run_experiments


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
parser.add_argument("--experiment-name", default="default", type=str,
                    help="Name of experiment dir.")
parser.add_argument("-f", "--config-file", default=None, type=str,
                    help="If specified, use config options from this file.")


if __name__ == "__main__":
    args = parser.parse_args(sys.argv[1:])
    if args.config_file:
        with open(args.config_file) as f:
            experiments = yaml.load(f)
    else:
        experiments = {
            args.experiment_name: {  # i.e. log to /tmp/ray/default
                "alg": args.alg,
                "env": args.env,
                "resources": resources_to_json(args.resources),
                "stop": args.stop,
                "config": args.config,
                "restore": args.restore,
                "repeat": args.repeat,
            }
        }

    for exp in experiments.values():
        if not exp.get("alg"):
            parser.error("the following arguments are required: --alg")
        if not exp.get("env"):
            parser.error("the following arguments are required: --env")

    run_experiments(
        experiments, scheduler=make_scheduler(args),
        redis_address=args.redis_address,
        num_cpus=args.num_cpus, num_gpus=args.num_gpus)

#!/usr/bin/env python

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import sys
import yaml

from ray.tune.config_parser import make_parser, resources_to_json
from ray.tune.tune import _make_scheduler, run_experiments


EXAMPLE_USAGE = """
Training example:
    ./train.py --run DQN --env CartPole-v0

Grid search example:
    ./train.py -f tuned_examples/cartpole-grid-search-example.yaml

Note that -f overrides all other trial-specific command-line options.
"""


parser = make_parser(
    formatter_class=argparse.RawDescriptionHelpFormatter,
    description="Train a reinforcement learning agent.",
    epilog=EXAMPLE_USAGE)

# See also the base parser definition in ray/tune/config_parser.py
parser.add_argument(
    "--redis-address", default=None, type=str,
    help="The Redis address of the cluster.")
parser.add_argument(
    "--num-cpus", default=None, type=int,
    help="Number of CPUs to allocate to Ray.")
parser.add_argument(
    "--num-gpus", default=None, type=int,
    help="Number of GPUs to allocate to Ray.")
parser.add_argument(
    "--experiment-name", default="default", type=str,
    help="Name of the subdirectory under `local_dir` to put results in.")
parser.add_argument(
    "--env", default=None, type=str, help="The gym environment to use.")
parser.add_argument(
    "-f", "--config-file", default=None, type=str,
    help="If specified, use config options from this file. Note that this "
    "overrides any trial-specific options set via flags above.")


if __name__ == "__main__":
    args = parser.parse_args(sys.argv[1:])
    if args.config_file:
        with open(args.config_file) as f:
            experiments = yaml.load(f)
    else:
        # Note: keep this in sync with tune/config_parser.py
        experiments = {
            args.experiment_name: {  # i.e. log to /tmp/ray/default
                "run": args.run,
                "checkpoint_freq": args.checkpoint_freq,
                "local_dir": args.local_dir,
                "resources": resources_to_json(args.resources),
                "stop": args.stop,
                "config": dict(args.config, env=args.env),
                "restore": args.restore,
                "repeat": args.repeat,
                "upload_dir": args.upload_dir,
            }
        }

    for exp in experiments.values():
        if not exp.get("run"):
            parser.error("the following arguments are required: --run")
        if not exp.get("env") and not exp.get("config", {}).get("env"):
            parser.error("the following arguments are required: --env")

    run_experiments(
        experiments, scheduler=_make_scheduler(args),
        redis_address=args.redis_address,
        num_cpus=args.num_cpus, num_gpus=args.num_gpus)

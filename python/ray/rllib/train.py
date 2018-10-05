#!/usr/bin/env python

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import yaml
import logging
import copy

import ray
from ray.tune.config_parser import make_parser, resources_to_json
from ray.tune.tune import _make_scheduler, run_experiments

logger = logging.getLogger(__name__)

EXAMPLE_USAGE = """
Training example via RLlib CLI:
    rllib train --run DQN --env CartPole-v0

Grid search example via RLlib CLI:
    rllib train -f tuned_examples/cartpole-grid-search-example.yaml

Grid search example via executable:
    ./train.py -f tuned_examples/cartpole-grid-search-example.yaml

Note that -f overrides all other trial-specific command-line options.
"""

DEFAULT_NAME = "default"


def create_parser(parser_creator=None):
    parser = make_parser(
        parser_creator=parser_creator,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="Train a reinforcement learning agent.",
        epilog=EXAMPLE_USAGE)

    # See also the base parser definition in ray/tune/config_parser.py
    parser.add_argument(
        "--redis-address",
        default=None,
        type=str,
        help="The Redis address of the cluster.")
    parser.add_argument(
        "--ray-num-cpus",
        default=None,
        type=int,
        help="--num-cpus to pass to Ray."
        " This only has an affect in local mode.")
    parser.add_argument(
        "--ray-num-gpus",
        default=None,
        type=int,
        help="--num-gpus to pass to Ray."
        " This only has an affect in local mode.")
    parser.add_argument(
        "--experiment-name",
        default=DEFAULT_NAME,
        type=str,
        help="Name of the subdirectory under `local_dir` to put results in.")
    parser.add_argument(
        "--env", default=None, type=str, help="The gym environment to use.")
    parser.add_argument(
        "--queue-trials",
        action='store_true',
        help=(
            "Whether to queue trials when the cluster does not currently have "
            "enough resources to launch one. This should be set to True when "
            "running on an autoscaling cluster to enable automatic scale-up."))
    parser.add_argument(
        "-f",
        "--config-file",
        default=None,
        type=str,
        help="If specified, use config options from this file. Note that this "
        "overrides any trial-specific options set via flags above.")
    return parser

def _args_to_config(args):
    """Converts parsed args to experiment dict.

    Note: keep this in sync with tune/config_parser.py

    Args:
        args (Namespace): Parsed args from argparse.
    """
    return {
        "run": args.run,
        "checkpoint_freq": args.checkpoint_freq,
        "local_dir": args.local_dir,
        "trial_resources": (
            args.trial_resources and
            resources_to_json(args.trial_resources)),
        "stop": args.stop,
        "config": dict(args.config, env=args.env),
        "restore": args.restore,
        "num_samples": args.num_samples,
        "upload_dir": args.upload_dir
    }


def run(args, parser):
    if args.config_file:
        if args.experiment_name != DEFAULT_NAME:
            logger.warn("Ignoring experiment-name since config is provided.")
        with open(args.config_file) as f:
            config_experiments = yaml.load(f)

        experiments = {}
        for name, values in config_experiments.items():
            # Override config file settings with command-line settings.
            # We do this by creating a new parser and setting the defaults
            # of the parser to be the config file settings, and then
            # overriding these defaults with `parse_known_args`.
            new_parser = create_parser()
            new_parser.set_defaults(**values)
            overridden = new_parser.parse_known_args()[0]
            experiments[name] = _args_to_config(overridden)

    else:
        experiments = {args.experiment_name: _args_to_config(args)}

    for exp in experiments.values():
        if not exp.get("run"):
            parser.error("the following arguments are required: --run")
        if not exp.get("env") and not exp.get("config", {}).get("env"):
            parser.error("the following arguments are required: --env")

    ray.init(
        redis_address=args.redis_address,
        num_cpus=args.ray_num_cpus,
        num_gpus=args.ray_num_gpus)
    run_experiments(
        experiments,
        scheduler=_make_scheduler(args),
        queue_trials=args.queue_trials)


if __name__ == "__main__":
    parser = create_parser()
    args = parser.parse_args()
    run(args, parser)

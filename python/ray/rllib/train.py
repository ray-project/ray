#!/usr/bin/env python

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import json
import os
import pprint
import sys
import yaml

import ray
from ray.tune.config_parser import make_parser, parse_to_trials
from ray.tune.trial_runner import TrialRunner
from ray.tune.trial import Trial, TERMINATED


parser = make_parser("Train a reinforcement learning agent.")

# Extends the base parser defined in ray/tune/config_parser, to add some
# RLlib specific arguments.
parser.add_argument("--redis-address", default=None, type=str,
                    help="The Redis address of the cluster.")
parser.add_argument("--restore", default=None, type=str,
                    help="If specified, restore from this checkpoint.")
parser.add_argument("-f", "--config-file", default=None, type=str,
                    help="If specified, use config options from this file.")


if __name__ == "__main__":
    args = parser.parse_args()
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
                args.resources, args.stop, args.checkpoint_freq,
                args.restore))

    ray.init(redis_address=args.redis_address)

    while not runner.is_finished():
        runner.step()
        print(runner.debug_string())

    for trial in runner.get_trials():
        if trial.status != TERMINATED:
            sys.exit(1)

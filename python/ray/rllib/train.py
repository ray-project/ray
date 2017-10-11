#!/usr/bin/env python

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import json
import os
import pprint
import sys

import ray
from ray.tune.config_parser import make_parser
from ray.tune.trial_runner import TrialRunner
from ray.tune.trial import Trial, TERMINATED


parser = make_parser("Train a reinforcement learning agent.")

# Extends the base parser defined in ray/tune/config_parser, to add some
# RLlib specific arguments.
parser.add_argument("--env", required=True, type=str,
                    help="The gym environment to use.")
parser.add_argument("--redis-address", default=None, type=str,
                    help="The Redis address of the cluster.")
parser.add_argument("--restore", default=None, type=str,
                    help="If specified, restore from this checkpoint.")


if __name__ == "__main__":
    args = parser.parse_args()

    ray.init(redis_address=args.redis_address)

    runner = TrialRunner()
    runner.add_trial(
        Trial(
            args.env, args.alg, args.config, args.local_dir, None,
            args.resources, args.stop, args.checkpoint_freq,
            args.restore))

    while not runner.is_finished():
        runner.process_results()
        print(runner.debug_string())

    for trial in runner.get_trials():
        assert trial.status == TERMINATED, trial

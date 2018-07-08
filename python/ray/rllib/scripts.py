#!/usr/bin/env python

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import sys
import yaml

from ray.rllib import train
from ray.rllib import rollout


EXAMPLE_USAGE = """
Example usage for training:
    rllib train --run DQN --env CartPole-v0

Example usage for rollout:
    rllib rollout /tmp/ray/checkpoint_dir/checkpoint-0 --run DQN
"""


def cli():
    parser = argparse.ArgumentParser(
        description="Train or Run an RLlib Agent.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=EXAMPLE_USAGE)
    cmd_sp = parser.add_subparsers(
        help="Commands to train or run an RLlib agent.", dest="command")

    # see _SubParsersAction.add_parser in
    # https://github.com/python/cpython/blob/master/Lib/argparse.py
    train.create_parser(lambda **kwargs: cmd_sp.add_parser("train", **kwargs))
    rollout.create_parser(lambda **kwargs: cmd_sp.add_parser("rollout", **kwargs))
    options = parser.parse_args()

    if options.command == "train":
        train.run(options)
    elif options.command == "rollout":
        rollout.run(options)
    else:
        parser.print_help()

#!/usr/bin/env python

import argparse

from ray.rllib import evaluate, train
from ray.rllib.utils.deprecation import deprecation_warning

EXAMPLE_USAGE = """
Example usage for training:
    rllib train --run DQN --env CartPole-v0

Example usage for evaluate (aka: "rollout"):
    rllib evaluate /trial_dir/checkpoint_000001/checkpoint-1 --run DQN
"""


def cli():
    parser = argparse.ArgumentParser(
        description="Train or evaluate an RLlib Algorithm.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=EXAMPLE_USAGE,
    )
    subcommand_group = parser.add_subparsers(
        help="Commands to train or evaluate an RLlib agent.", dest="command"
    )

    # see _SubParsersAction.add_parser in
    # https://github.com/python/cpython/blob/master/Lib/argparse.py
    train_parser = train.create_parser(
        lambda **kwargs: subcommand_group.add_parser("train", **kwargs)
    )
    evaluate_parser = evaluate.create_parser(
        lambda **kwargs: subcommand_group.add_parser("evaluate", **kwargs)
    )
    rollout_parser = evaluate.create_parser(
        lambda **kwargs: subcommand_group.add_parser("rollout", **kwargs)
    )
    options = parser.parse_args()

    if options.command == "train":
        train.run(options, train_parser)
    elif options.command == "evaluate":
        evaluate.run(options, evaluate_parser)
    elif options.command == "rollout":
        deprecation_warning(old="rllib rollout", new="rllib evaluate", error=False)
        evaluate.run(options, rollout_parser)
    else:
        parser.print_help()

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
import ray.rllib.ppo as ppo
import ray.rllib.es as es
import ray.rllib.dqn as dqn
import ray.rllib.a3c as a3c

parser = argparse.ArgumentParser(
    description=("Train a reinforcement learning agent."))
parser.add_argument("--redis-address", default=None, type=str,
                    help="The Redis address of the cluster.")
parser.add_argument("--env", required=True, type=str,
                    help="The gym environment to use.")
parser.add_argument("--alg", required=True, type=str,
                    help="The reinforcement learning algorithm to use.")
parser.add_argument("--num-iterations", default=sys.maxsize, type=int,
                    help="The number of training iterations to run.")
parser.add_argument("--config", default="{}", type=str,
                    help="The configuration options of the algorithm.")
parser.add_argument("--upload-dir", default="file:///tmp/ray", type=str,
                    help="Where the traces are stored.")
parser.add_argument("--checkpoint-freq", default=sys.maxsize, type=int,
                    help="How many iterations between checkpoints.")
parser.add_argument("--restore", default="", type=str,
                    help="If specified, restores state from this checkpoint.")


if __name__ == "__main__":
    args = parser.parse_args()
    json_config = json.loads(args.config)

    ray.init(redis_address=args.redis_address)

    def _check_and_update(config, json):
        for k in json.keys():
            if k not in config:
                raise Exception(
                    "Unknown model config `{}`, all model configs: {}".format(
                        k, config.keys()))
        config.update(json)

    env_name = args.env
    if args.alg == "PPO":
        config = ppo.DEFAULT_CONFIG.copy()
        _check_and_update(config, json_config)
        alg = ppo.PPOAgent(
            env_name, config, upload_dir=args.upload_dir)
    elif args.alg == "ES":
        config = es.DEFAULT_CONFIG.copy()
        _check_and_update(config, json_config)
        alg = es.ESAgent(
            env_name, config, upload_dir=args.upload_dir)
    elif args.alg == "DQN":
        config = dqn.DEFAULT_CONFIG.copy()
        _check_and_update(config, json_config)
        alg = dqn.DQNAgent(
            env_name, config, upload_dir=args.upload_dir)
    elif args.alg == "A3C":
        config = a3c.DEFAULT_CONFIG.copy()
        _check_and_update(config, json_config)
        alg = a3c.A3CAgent(
            env_name, config, upload_dir=args.upload_dir)
    else:
        assert False, ("Unknown algorithm, check --alg argument. Valid "
                       "choices are PPO, ES, DQN and A3C.")

    if args.restore:
        alg.restore(args.restore)

    for i in range(args.num_iterations):
        result = alg.train()

        print("== Iteration {} ==".format(alg.iteration))
        pprint.pprint(result._asdict())

        if (i + 1) % args.checkpoint_freq == 0:
            print("checkpoint path: {}".format(alg.save()))

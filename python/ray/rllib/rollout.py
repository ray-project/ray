#!/usr/bin/env python

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import json
import os
import pickle

import gym
import ray
from ray.rllib.agent import get_agent_class
from ray.rllib.models import ModelCatalog
from ray.tune.registry import get_registry

EXAMPLE_USAGE = """
example usage:
    ./rollout.py /tmp/ray/checkpoint_dir/checkpoint-0 --run DQN """
"""--env CartPole-v0 --steps 1000000 --out rollouts.pkl
"""

parser = argparse.ArgumentParser(
    formatter_class=argparse.RawDescriptionHelpFormatter,
    description="Roll out a reinforcement learning agent "
                "given a checkpoint.", epilog=EXAMPLE_USAGE)

parser.add_argument(
    "checkpoint", type=str, help="Checkpoint from which to roll out.")
required_named = parser.add_argument_group("required named arguments")
required_named.add_argument(
    "--run", type=str, required=True,
    help="The algorithm or model to train. This may refer to the name "
         "of a built-on algorithm (e.g. RLLib's DQN or PPO), or a "
         "user-defined trainable function or class registered in the "
         "tune registry.")
required_named.add_argument(
    "--env", type=str, help="The gym environment to use.")
parser.add_argument(
    "--no-render", default=False, action="store_const", const=True,
    help="Surpress rendering of the environment.")
parser.add_argument(
    "--steps", default=None, help="Number of steps to roll out.")
parser.add_argument(
    "--out", default=None, help="Output filename.")
parser.add_argument(
    "--config", default="{}", type=json.loads,
    help="Algorithm-specific configuration (e.g. env, hyperparams). "
         "Surpresses loading of configuration from checkpoint.")

if __name__ == "__main__":
    args = parser.parse_args()

    if not args.config:
        # Load configuration from file
        config_dir = os.path.dirname(args.checkpoint)
        config_path = os.path.join(config_dir, "params.json")
        with open(config_path) as f:
            args.config = json.load(f)

    if not args.env:
        if not args.config.get("env"):
            parser.error("the following arguments are required: --env")
        args.env = args.config.get("env")

    ray.init()

    cls = get_agent_class(args.run)
    agent = cls(env=args.env)
    agent.restore(args.checkpoint)
    num_steps = int(args.steps)

    env = ModelCatalog.get_preprocessor_as_wrapper(get_registry(),
                                                   gym.make(args.env))
    if args.out is not None:
        rollouts = []
    steps = 0
    while steps < (num_steps or steps + 1):
        if args.out is not None:
            rollout = []
        state = env.reset()
        done = False
        while not done and steps < (num_steps or steps + 1):
            action = agent.compute_action(state)
            next_state, reward, done, _ = env.step(action)
            if not args.no_render:
                env.render()
            if args.out is not None:
                rollout.append([state, action, next_state, reward, done])
            steps += 1
        if args.out is not None:
            rollouts.append(rollout)
    if args.out is not None:
        pickle.dump(rollouts, open(args.out, "wb"))

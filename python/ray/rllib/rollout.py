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
from ray.rllib.agents.agent import get_agent_class
from ray.rllib.models import ModelCatalog

EXAMPLE_USAGE = """
Example Usage via RLlib CLI:
    rllib rollout /tmp/ray/checkpoint_dir/checkpoint-0 --run DQN
    --env CartPole-v0 --steps 1000000 --out rollouts.pkl

Example Usage via executable:
    ./rollout.py /tmp/ray/checkpoint_dir/checkpoint-0 --run DQN
    --env CartPole-v0 --steps 1000000 --out rollouts.pkl
"""


def create_parser(parser_creator=None):
    parser_creator = parser_creator or argparse.ArgumentParser
    parser = parser_creator(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="Roll out a reinforcement learning agent "
        "given a checkpoint.",
        epilog=EXAMPLE_USAGE)

    parser.add_argument(
        "checkpoint", type=str, help="Checkpoint from which to roll out.")
    required_named = parser.add_argument_group("required named arguments")
    required_named.add_argument(
        "--run",
        type=str,
        required=True,
        help="The algorithm or model to train. This may refer to the name "
        "of a built-on algorithm (e.g. RLLib's DQN or PPO), or a "
        "user-defined trainable function or class registered in the "
        "tune registry.")
    required_named.add_argument(
        "--env", type=str, help="The gym environment to use.")
    parser.add_argument(
        "--no-render",
        default=False,
        action="store_const",
        const=True,
        help="Surpress rendering of the environment.")
    parser.add_argument(
        "--steps", default=10000, help="Number of steps to roll out.")
    parser.add_argument("--out", default=None, help="Output filename.")
    parser.add_argument(
        "--config",
        default="{}",
        type=json.loads,
        help="Algorithm-specific configuration (e.g. env, hyperparams). "
        "Surpresses loading of configuration from checkpoint.")
    return parser


def run(args, parser):
    config = args.config
    if not config:
        # Load configuration from file
        config_dir = os.path.dirname(args.checkpoint)
        config_path = os.path.join(config_dir, "params.json")
        if not os.path.exists(config_path):
            config_path = os.path.join(config_dir, "../params.json")
        if not os.path.exists(config_path):
            raise ValueError(
                "Could not find params.json in either the checkpoint dir or "
                "its parent directory.")
        with open(config_path) as f:
            config = json.load(f)
        if "num_workers" in config:
            config["num_workers"] = min(2, config["num_workers"])

    if not args.env:
        if not config.get("env"):
            parser.error("the following arguments are required: --env")
        args.env = config.get("env")

    ray.init()

    cls = get_agent_class(args.run)
    agent = cls(env=args.env, config=config)
    agent.restore(args.checkpoint)
    num_steps = int(args.steps)

    if hasattr(agent, "local_evaluator"):
        env = agent.local_evaluator.env
    else:
        env = ModelCatalog.get_preprocessor_as_wrapper(gym.make(args.env))
    if args.out is not None:
        rollouts = []
    steps = 0
    while steps < (num_steps or steps + 1):
        if args.out is not None:
            rollout = []
        state = env.reset()
        done = False
        reward_total = 0.0
        while not done and steps < (num_steps or steps + 1):
            action = agent.compute_action(state)
            next_state, reward, done, _ = env.step(action)
            reward_total += reward
            if not args.no_render:
                env.render()
            if args.out is not None:
                rollout.append([state, action, next_state, reward, done])
            steps += 1
            state = next_state
        if args.out is not None:
            rollouts.append(rollout)
        print("Episode reward", reward_total)
    if args.out is not None:
        pickle.dump(rollouts, open(args.out, "wb"))


if __name__ == "__main__":
    parser = create_parser()
    args = parser.parse_args()
    run(args, parser)

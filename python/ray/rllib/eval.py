#!/usr/bin/env python

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import gym
import ray

from agent import get_agent_class


EXAMPLE_USAGE = """
example usage:
    ./train.py /tmp/ray/checkpoint_dir/checkpoint-0 --run DQN --env CartPole-v0
"""


parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="Evaluates a reinforcement learning agent "
        "given a checkpoint.", epilog=EXAMPLE_USAGE)

parser.add_argument("checkpoint", type=str,
                    help="Checkpoint from which to evaluate.")
required_named = parser.add_argument_group("required named arguments")
required_named.add_argument(
        "--run", type=str, required=True,
        help="The algorithm or model to train. This may refer to the name "
        "of a built-on algorithm (e.g. RLLib's DQN or PPO), or a "
        "user-defined trainable function or class registered in the "
        "tune registry.")
required_named.add_argument(
    "--env", type=str, required=True, help="The gym environment to use.")
parser.add_argument(
    "--hide", default=False, action="store_const", const=True,
    help="Surpress rendering of the environment.")


if __name__ == "__main__":
    args = parser.parse_args()
    ray.init()

    cls = get_agent_class(args.run)
    agent = cls(env=args.env)
    agent._restore(args.checkpoint)

    env = gym.make(args.env)
    state = env.reset()
    done = False
    while not done:
        action = agent.compute_action(state)
        state, reward, done, _ = env.step(action)
        if not args.hide:
            env.render()

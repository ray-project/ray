"""
8 Environments from D4RL Environment.
Use fully qualified class-path in your configs:
e.g. "env": "ray.rllib.examples.envs.classes.d4rl_env.halfcheetah_random".
"""

import gymnasium as gym

try:
    import d4rl

    _ = d4rl.__name__  # Fool LINTer.
except ImportError:
    d4rl = None


def halfcheetah_random():
    return gym.make("halfcheetah-random-v0")


def halfcheetah_medium():
    return gym.make("halfcheetah-medium-v0")


def halfcheetah_expert():
    return gym.make("halfcheetah-expert-v0")


def halfcheetah_medium_replay():
    return gym.make("halfcheetah-medium-replay-v0")


def hopper_random():
    return gym.make("hopper-random-v0")


def hopper_medium():
    return gym.make("hopper-medium-v0")


def hopper_expert():
    return gym.make("hopper-expert-v0")


def hopper_medium_replay():
    return gym.make("hopper-medium-replay-v0")

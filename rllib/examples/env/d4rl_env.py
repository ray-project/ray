"""
4 Environments from D4RL Environment
"""
import gym
import d4rl

d4rl.__name__  # Fool LINTer.


def halfcheetah_random():
    return gym.make("halfcheetah-random-v1")


def halfcheetah_medium():
    return gym.make("halfcheetah-medium-v1")


def hopper_random():
    return gym.make("hopper-random-v1")


def hopper_medium():
    return gym.make("hopper-medium-v1")

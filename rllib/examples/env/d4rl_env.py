import gym
"""
4 Environments from D4RL Environment
"""

def halfcheetah_random():
    import d4rl
    return gym.make("halfcheetah-random-v1")


def halfcheetah_medium():
    import d4rl
    return gym.make("halfcheetah-medium-v1")


def hopper_random():
    import d4rl
    return gym.make("hopper-random-v1")


def hopper_medium():
    import d4rl
    return gym.make("hopper-medium-v1")



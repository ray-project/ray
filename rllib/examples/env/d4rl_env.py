import gym
"""
4 Environments from D4RL Environment
"""

def halfcheetah_random():
    import d4rl
    return gym.make("halfcheetah-random-v0")


def halfcheetah_medium():
    import d4rl
    return gym.make("halfcheetah-medium-v0")


def halfcheetah_expert():
    import d4rl
    return gym.make("halfcheetah-expert-v0")


def halfcheetah_medium_replay():
    import d4rl
    return gym.make("halfcheetah-medium-replay-v0")


def hopper_random():
    import d4rl
    return gym.make("hopper-random-v0")


def hopper_medium():
    import d4rl
    return gym.make("hopper-medium-v0")


def hopper_expert():
    import d4rl
    return gym.make("hopper-expert-v0")


def hopper_medium_replay():
    import d4rl
    return gym.make("hopper-medium-replay-v0")



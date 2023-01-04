import gymnasium as gym


def create_cartpole_deterministic(seed=0):
    env = gym.make("CartPole-v1")
    env.reset(seed=seed)
    return env


def create_pendulum_deterministic(seed=0):
    env = gym.make("Pendulum-v1")
    env.reset(seed=seed)
    return env

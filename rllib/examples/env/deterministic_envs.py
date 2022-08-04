import gym


class DeterministicCartPole(gym.Env):
    def __init__(self, seed=0):
        self.env = gym.make("CartPole-v0")
        self.env.seed(seed)
        self.action_space = self.env.action_space
        self.observation_space = self.env.observation_space

    def reset(self):
        return self.env.reset()

    def step(self, action):
        return self.env.step(action)


class DeterministicPendulum(gym.Env):
    def __init__(self, seed=0):
        self.env = gym.make("Pendulum-v1")
        self.env.seed(seed)
        self.action_space = self.env.action_space
        self.observation_space = self.env.observation_space

    def reset(self):
        return self.env.reset()

    def step(self, action):
        return self.env.step(action)

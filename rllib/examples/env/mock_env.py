import gym


class MockEnv(gym.Env):
    """Mock environment for testing purposes.

    Observation=0, reward=1.0, episode-len is configurable.
    Actions are ignored.
    """

    def __init__(self, episode_length, config=None):
        self.episode_length = episode_length
        self.config = config
        self.i = 0
        self.observation_space = gym.spaces.Discrete(1)
        self.action_space = gym.spaces.Discrete(2)

    def reset(self):
        self.i = 0
        return 0

    def step(self, action):
        self.i += 1
        return 0, 1.0, self.i >= self.episode_length, {}


class MockEnv2(gym.Env):
    """Mock environment for testing purposes.

    Observation=ts (discrete space!), reward=100.0, episode-len is
    configurable. Actions are ignored.
    """

    def __init__(self, episode_length):
        self.episode_length = episode_length
        self.i = 0
        self.observation_space = gym.spaces.Discrete(100)
        self.action_space = gym.spaces.Discrete(2)

    def reset(self):
        self.i = 0
        return self.i

    def step(self, action):
        self.i += 1
        return self.i, 100.0, self.i >= self.episode_length, {}

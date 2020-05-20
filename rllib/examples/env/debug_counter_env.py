import gym


class DebugCounterEnv(gym.Env):
    """Simple Env that yields a ts counter as observation (0-based).

    Actions have no effect.
    The episode length is always 15.
    Reward is always: current ts % 3.
    """

    def __init__(self):
        self.action_space = gym.spaces.Discrete(2)
        self.observation_space = gym.spaces.Box(0, 100, (1, ))
        self.i = 0

    def reset(self):
        self.i = 0
        return [self.i]

    def step(self, action):
        self.i += 1
        return [self.i], self.i % 3, self.i >= 15, {}

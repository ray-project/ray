import gym
from gym import spaces
from gym.utils import seeding


class RouletteEnv(gym.Env):
    """Simple roulette environment

    The roulette wheel has 37 spots. If the bet is 0 and a 0 comes up,
    you win a reward of 35. If the parity of your bet matches the parity
    of the spin, you win 1. Otherwise you receive a reward of -1.

    The long run reward for playing 0 should be -1/37 for any state

    The last action (38) stops the rollout for a return of 0 (walking away)
    """
    def __init__(self, spots=37):
        self.n = spots + 1
        self.action_space = spaces.Discrete(self.n)
        self.observation_space = spaces.Discrete(1)
        self.seed()

    def seed(self, seed=None):
        self.np_random, seed = seeding.np_random(seed)
        return [seed]

    def step(self, action):
        assert self.action_space.contains(action)
        if action == self.n - 1:
            # observation, reward, done, info
            return 0, 0, True, {}

        # N.B. np.random.randint draws from [A, B) while random.randint draws from [A,B]
        val = self.np_random.randint(0, self.n - 1)
        if val == action == 0:
            reward = self.n - 2.0
        elif val != 0 and action != 0 and val % 2 == action % 2:
            reward = 1.0
        else:
            reward = -1.0
        return 0, reward, False, {}

    def reset(self):
        return 0

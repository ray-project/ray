from gym.spaces import Tuple
from gym.envs.classic_control.pendulum import PendulumEnv

"""
Single agent pendulum that spoofs being a multiagent pendulum
"""


class AlteredPendulumEnv(PendulumEnv):
    metadata = {
      'render.modes': ['human', 'rgb_array'],
      'video.frames_per_second': 30
    }

    def __init__(self):
        super(AlteredPendulumEnv, self).__init__()
        self.action_space = [self.action_space]
        self.observation_space = Tuple([self.observation_space])

    def step(self, u):
        action = u[0][0]
        state, rew, done, _ = super(AlteredPendulumEnv, self).step(action)
        return [state], rew, done, {}

    def reset(self):
        obs = super(AlteredPendulumEnv, self).reset()
        return [obs]

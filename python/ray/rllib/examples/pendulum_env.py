from gym.spaces import Box, Tuple
from gym.utils import seeding
from gym.envs.classic_control.pendulum import PendulumEnv
import numpy as np

"""
 Multiagent pendulum that sums its torques to generate an action
"""


class AlteredPendulumEnv(PendulumEnv):
    metadata = {
      'render.modes': ['human', 'rgb_array'],
      'video.frames_per_second': 30
    }

    def __init__(self):
        super().__init__()
        self.action_space = [Box(low=-self.max_torque,
                                 high=self.max_torque,
                                 shape=(1,),
                                 dtype=np.float32)
                             for _ in range(2)]
        self.observation_space = Tuple([
            Box(low=-high, high=high, dtype=np.float32)])
